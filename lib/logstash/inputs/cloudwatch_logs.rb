# encoding: utf-8
require "logstash/inputs/base"
require "logstash/namespace"
require "logstash/plugin_mixins/aws_config"
require "logstash/timestamp"
require "time"
require "tmpdir"
require "stud/interval"
require "stud/temporary"
require "aws-sdk"
require "logstash/inputs/cloudwatch/patch"

Aws.eager_autoload!

# Stream events from CloudWatch Logs streams.
#
# Specify an individual log group, and this plugin will scan
# all log streams in that group, and pull in any new log events.
#
# Optionally, you may set the `log_group_prefix` parameter to true
# which will scan for all log groups matching the specified prefix
# and ingest all logs available in all of the matching groups.
#
class LogStash::Inputs::CloudWatch_Logs < LogStash::Inputs::Base
  include LogStash::PluginMixins::AwsConfig::V2

  config_name "cloudwatch_logs"

  default :codec, "plain"

  # Log group to pull logs from for this plugin. Will pull in all
  # streams inside of this log group.
  config :log_group, :validate => :string, :required => true

  # Where to write the since database (keeps track of the date
  # the last handled log stream was updated). The default will write
  # sincedb files to some path matching "$HOME/.sincedb*"
  # Should be a path with filename not just a directory.
  config :sincedb_path, :validate => :string, :default => nil

  # Interval to wait between to check the file list again after a run is finished.
  # Value is in seconds.
  config :interval, :validate => :number, :default => 60

  # Decide if log_group is a prefix or an absolute name
  config :log_group_prefix, :validate => :boolean, :default => false

  # Number of hours back from which we are fetching the logs
  config :buffer, :validate => :number, :default => 1

  # def register
  public
  def register
    require "digest/md5"

    @logger.info("Registering cloudwatch_logs input", :log_group => @log_group)

    Aws::ConfigService::Client.new(aws_options_hash)

    @cloudwatch = Aws::CloudWatchLogs::Client.new(aws_options_hash)
  end #def register

  # def run
  public
  def run(queue)
    while !stop?
      process_group(queue)
      Stud.stoppable_sleep(@interval)
    end
  end # def run

  # def list_new_streams
  public
  def list_new_streams()
    if @log_group_prefix
      log_groups = @cloudwatch.describe_log_groups(log_group_name_prefix: @log_group)
      groups = log_groups.log_groups.map {|n| n.log_group_name}
      while log_groups.next_token
        log_groups = @cloudwatch.describe_log_groups(log_group_name_prefix: @log_group, next_token: log_groups.next_token)
        groups += log_groups.log_groups.map {|n| n.log_group_name}
      end
    else
      groups = [@log_group]
    end
    objects = []
    for log_group in groups
      objects.concat(list_new_streams_for_log_group(log_group))
    end
    objects
  end

  # def list_new_streams_for_log_group
  public
  def list_new_streams_for_log_group(log_group, token = nil, objects = [], stepback=0)
    params = {
      :log_group_name => log_group,
      :order_by => "LastEventTime",
      :descending => false
    }

    @logger.debug("CloudWatch Logs for log_group #{log_group}")

    if token != nil
      params[:next_token] = token
    end

    begin
      streams = @cloudwatch.describe_log_streams(params)
    rescue Aws::CloudWatchLogs::Errors::ThrottlingException
      @logger.debug("CloudWatch Logs stepping back ", :stepback => 2 ** stepback * 60)
      sleep(2 ** stepback * 60)
      stepback += 1
      @logger.debug("CloudWatch Logs repeating list_new_streams again with token", :token => token)
      return list_new_streams_for_log_group(log_group, token=token, objects=objects, stepback=stepback)
    end

    objects.push(*streams.log_streams)
    if streams.next_token == nil || (streams[-1].last_event_timestamp < (Time.now - 60*60*@buffer).to_i*1000)
      @logger.debug("CloudWatch Logs hit end of tokens for streams")
      objects
    else
      @logger.debug("CloudWatch Logs calling list_new_streams again on token", :token => streams.next_token)
      list_new_streams_for_log_group(log_group, streams.next_token, objects)
    end
  end # def list_new_streams_for_log_group

  # def process_log
  private
  def process_log(queue, log, stream)

    @codec.decode(log.message.to_str) do |event|
      event.set("@timestamp", parse_time(log.timestamp))
      event.set("[cloudwatch][ingestion_time]", parse_time(log.ingestion_time))
      event.set("[cloudwatch][log_group]", stream.arn.split(/:/)[6])
      event.set("[cloudwatch][log_stream]", stream.log_stream_name)
      decorate(event)

      queue << event
    end
  end
  # def process_log

  # def parse_time
  private
  def parse_time(data)
    LogStash::Timestamp.at(data.to_i / 1000, (data.to_i % 1000) * 1000)
  end # def parse_time

  # def process_group
  public
  def process_group(queue)
    objects = list_new_streams

    last_read = sincedb.read
    current_window = DateTime.now.strftime('%Q')

    if last_read < 0
      last_read = 1
    end

    objects.each do |stream|
      if stream.last_ingestion_time && stream.last_ingestion_time > last_read
        process_log_stream(queue, stream, last_read, current_window)
      end
    end

    sincedb.write(current_window)
  end # def process_group

  # def process_log_stream
  private
  def process_log_stream(queue, stream, last_read, current_window, token = nil, stepback=0)
    @logger.debug("CloudWatch Logs processing stream",
                  :log_stream => stream.log_stream_name,
                  :log_group => stream.arn.split(":")[6],
                  :lastRead => last_read,
                  :currentWindow => current_window,
                  :token => token
    )

    params = {
        :log_group_name => stream.arn.split(":")[6],
        :log_stream_name => stream.log_stream_name,
        :start_from_head => true,
        :start_time => (Time.now.to_i - 60*@buffer) * 1000,
    }

    if token != nil
      params[:next_token] = token
    end


    begin
      logs = @cloudwatch.get_log_events(params)
    rescue Aws::CloudWatchLogs::Errors::ThrottlingException
      @logger.debug("CloudWatch Logs stepping back ", :stepback => 2 ** stepback * 60)
      sleep(2 ** stepback * 60)
      stepback += 1
      @logger.debug("CloudWatch Logs repeating process_log_stream again with token", :token => token)
      return process_log_stream(queue, stream, last_read, current_window, token, stepback)
    end

    logs.events.each do |log|
      if log.ingestion_time > last_read
        process_log(queue, log, stream)
      end
    end

    # if there are more pages, continue
    if logs.events.count != 0 && logs.next_forward_token != nil
      process_log_stream(queue, stream, last_read, current_window, logs.next_forward_token)
    end
  end # def process_log_stream

  private
  def sincedb
    @sincedb ||= if @sincedb_path.nil?
                   @logger.info("Using default generated file for the sincedb", :filename => sincedb_file)
                   SinceDB::File.new(sincedb_file)
                 else
                   @logger.info("Using the provided sincedb_path",
                                :sincedb_path => @sincedb_path)
                   SinceDB::File.new(@sincedb_path)
                 end
  end

  private
  def sincedb_file
    File.join(ENV["HOME"], ".sincedb_" + Digest::MD5.hexdigest("#{@log_group}"))
  end

  module SinceDB
    class File
      def initialize(file)
        @sincedb_path = file
      end

      def newer?(date)
        date > read
      end

      def read
        if ::File.exists?(@sincedb_path)
          since = ::File.read(@sincedb_path).chomp.strip.to_i
        else
          since = 1
        end
        return since
      end

      def write(since = nil)
        since = DateTime.now.strftime('%Q') if since.nil?
        ::File.open(@sincedb_path, 'w') { |file| file.write(since.to_s) }
      end
    end
  end
end # class LogStash::Inputs::CloudWatch_Logs
