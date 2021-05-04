# encoding: utf-8
require "logstash/inputs/base"
require "logstash/namespace"
require "logstash/plugin_mixins/aws_config"
require "logstash/timestamp"
require "time"
require "stud/interval"
require "aws-sdk"
require "logstash/inputs/cloudwatch_logs/patch"
require "fileutils"

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

  # Log group(s) to use as an input. If `log_group_prefix` is set
  # to `true`, then each member of the array is treated as a prefix
  config :log_group, :validate => :string, :list => true

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

  # Decide if present, then the results of the log group query are filtered again
  # to limit to these values. Only applicable if log_group_prefix = true 
  config :log_group_suffix, :validate => :string, :list => true, :default => nil  
  config :negate_log_group_suffix, :validate => :boolean, :default => false

  # When a new log group is encountered at initial plugin start (not already in
  # sincedb), allow configuration to specify where to begin ingestion on this group.
  # Valid options are: `beginning`, `end`, or an integer, representing number of
  # seconds before now to read back from.
  config :start_position, :default => 'beginning'


  # def register
  public
  def register
    require "digest/md5"
    @logger.debug("Registering cloudwatch_logs input", :log_group => @log_group)
    settings = defined?(LogStash::SETTINGS) ? LogStash::SETTINGS : nil
    @sincedb = {}

    check_start_position_validity

    Aws::ConfigService::Client.new(aws_options_hash)
    @cloudwatch = Aws::CloudWatchLogs::Client.new(aws_options_hash)

    if @sincedb_path.nil?
      if settings
        datapath = File.join(settings.get_value("path.data"), "plugins", "inputs", "cloudwatch_logs")
        # Ensure that the filepath exists before writing, since it's deeply nested.
        FileUtils::mkdir_p datapath
        @sincedb_path = File.join(datapath, ".sincedb_" + Digest::MD5.hexdigest(@log_group.join(",")))
      end
    end

    # This section is going to be deprecated eventually, as path.data will be
    # the default, not an environment variable (SINCEDB_DIR or HOME)
    if @sincedb_path.nil? # If it is _still_ nil...
      if ENV["SINCEDB_DIR"].nil? && ENV["HOME"].nil?
        @logger.error("No SINCEDB_DIR or HOME environment variable set, I don't know where " \
                      "to keep track of the files I'm watching. Either set " \
                      "HOME or SINCEDB_DIR in your environment, or set sincedb_path in " \
                      "in your Logstash config for the file input with " \
                      "path '#{@path.inspect}'")
        raise
      end

      #pick SINCEDB_DIR if available, otherwise use HOME
      sincedb_dir = ENV["SINCEDB_DIR"] || ENV["HOME"]

      @sincedb_path = File.join(sincedb_dir, ".sincedb_" + Digest::MD5.hexdigest(@log_group.join(",")))

      @logger.info("No sincedb_path set, generating one based on the log_group setting",
                   :sincedb_path => @sincedb_path, :log_group => @log_group)
    end

    @logger.info("Using sincedb_path #{@sincedb_path}")
  end #def register

  public
  def check_start_position_validity
    raise LogStash::ConfigurationError, "No start_position specified!" unless @start_position

    return if @start_position =~ /^(beginning|end)$/
    return if @start_position.is_a? Integer

    raise LogStash::ConfigurationError, "start_position '#{@start_position}' is invalid! Must be `beginning`, `end`, or an integer."
  end # def check_start_position_validity

  # def run
  public
  def run(queue)
    @queue = queue
    @priority = []
    _sincedb_open

    while !stop?
      begin
        groups = find_log_groups

        groups.each do |group|
          @logger.debug("calling process_group on #{group}")
          process_group(group)
        end # groups.each
      rescue Aws::CloudWatchLogs::Errors::ThrottlingException
        @logger.debug("reached rate limit")
      end

      Stud.stoppable_sleep(@interval) { stop? }
    end
  end # def run

  public
  def find_log_groups
    if @log_group_prefix
      @logger.debug("log_group prefix is enabled, searching for log groups")
      groups = []
      next_token = nil
      @log_group.each do |group|
        loop do
          log_groups = @cloudwatch.describe_log_groups(log_group_name_prefix: group, next_token: next_token)
          # if we have no suffix setting, or if the candidate group name ends with the suffix
          # we use it
          groups += log_groups.log_groups            
            .select { |n| @log_group_suffix.nil?  || (n.log_group_name.end_with?(*@log_group_suffix) ^ @negate_log_group_suffix)}
            .map {|n| n.log_group_name}
          
          next_token = log_groups.next_token
          @logger.debug("found #{log_groups.log_groups.length} log groups matching prefix #{group}")
          break if next_token.nil?
        end
      end
    else
      @logger.debug("log_group_prefix not enabled")
      groups = @log_group
    end
    # Move the most recent groups to the end
    groups.sort{|a,b| priority_of(a) <=> priority_of(b) }
  end # def find_log_groups

  private
  def priority_of(group)
    @priority.index(group) || -1
  end


  private
  def process_group(group)
    next_token = nil
    loop do

      # The log streams in the group can overlap, even if interleaved
      # is true (as it is by default) so we restart each query from the earliest
      # time for which we had records across the group.
      # We will still filter out old records at the stream level in  process_log
      # We don't filter by the log streams because we'd have to go looking for them
      # and AWS can't guarantee the last event time in the result of that query
      # is accurate

      # NOTE: if the group-level filter isn't set to anything it will
      # be set by this method to the correct default
      start_time, stream_positions = get_sincedb_group_values(group)

      params = {
          :log_group_name => group,
          :start_time => start_time,
          :interleaved => true,
          :next_token => next_token
      }
      resp = @cloudwatch.filter_log_events(params)

      resp.events.each do |event|
        process_log(event, group, stream_positions)
      end

      _sincedb_write

      next_token = resp.next_token
      break if next_token.nil?
    end
    @priority.delete(group)
    @priority << group
  end #def process_group

  # def process_log
  private
  def process_log(log, group, stream_positions)
    identity = identify(group, log.log_stream_name)
    stream_position = stream_positions.fetch(identity, 0)
    
    @logger.trace? && @logger.trace("Checking event time #{log.timestamp} (#{parse_time(log.timestamp)}) -> #{stream_position}")    

    if log.timestamp >= stream_position
      @logger.trace? && @logger.trace("Processing event")    
      @codec.decode(log.message.to_str) do |event|
        event.set("@timestamp", parse_time(log.timestamp))
        event.set("[cloudwatch_logs][ingestion_time]", parse_time(log.ingestion_time))
        event.set("[cloudwatch_logs][log_group]", group)
        event.set("[cloudwatch_logs][log_stream]", log.log_stream_name)
        event.set("[cloudwatch_logs][event_id]", log.event_id)
        decorate(event)

        @queue << event
        set_sincedb_value(group, log.log_stream_name, log.timestamp + 1 )
      end
    end
  end # def process_log

  # def parse_time
  private
  def parse_time(data)
    LogStash::Timestamp.at(data.to_i / 1000, (data.to_i % 1000) * 1000)
  end # def parse_time


  private 
  def identify(group, log_stream_name)
    # https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-resource-logs-loggroup.html
    # ':' isn't allowed in a log group name, so we can use it safely
    return "#{group}:#{log_stream_name}"
  end

  private
  def set_sincedb_value(group, log_stream_name, timestamp)
    @logger.debug("Setting sincedb #{group}:#{log_stream_name} -> #{timestamp}")    

    # make sure this is an integer
    if timestamp.is_a? String 
      timestamp = timestamp.to_i
    end

    # the per-stream level
    @sincedb[identify(group, log_stream_name)] = timestamp

    # the overall group high water mark
    current_pos = @sincedb.fetch(group, 0)
    if current_pos.is_a? String 
      current_pos = current_pos.to_i
    end

    if current_pos < timestamp
      @sincedb[group] = timestamp
    end

  end # def set_sincedb_value



  # scan over all the @sincedb entries for this group and pick the _earliest_ value
  # Returns a list of [earliest last position in group, the last position of each log stream in that group ]
  # the map is of the form [group:logstream] -> timestamp (long ms) and also [group] -> timestamp (long ms) 
  private
  def get_sincedb_group_values(group)
    # if we've no group, or stream, then we have never seen these events
    @logger.debug("Getting sincedb #{group} from  #{@sincedb}")

    # if we have no group-level then we set one
    if @sincedb[group].nil?
      @sincedb[group] = get_default_start_time
    end

    ## assume the group level min value
    min_value = @sincedb[group]
    min_map = {}
    # now route through all the entries for this group (one of these will
    # be the group-specific entry)
    @sincedb.map do |identity, pos|
      if identity.start_with?(group) && group != identity
        min_map[identity] = pos
        if (min_value.nil? || min_value > pos)
          min_value = pos
        end     
      end      
    end 

    @logger.debug("Got sincedb #{group} as #{min_value} -> #{min_map}")

    return [min_value, min_map]
  end # def get_sincedb_group_values  

  private
  def get_default_start_time()
    # chose the start time based on the configs
    case @start_position
    when 'beginning'
      return 0
    when 'end'
      return DateTime.now.strftime('%Q')
    else
      return DateTime.now.strftime('%Q').to_i - (@start_position * 1000)
    end # case @start_position    
  end

  private
  def _sincedb_open
    begin
      File.open(@sincedb_path) do |db|
        @logger.debug? && @logger.debug("_sincedb_open: reading from #{@sincedb_path}")
        db.each do |line|
          identity, pos = line.split(" ", 2)
          @logger.debug? && @logger.debug("_sincedb_open: setting #{identity} to #{pos.to_i}")
          @sincedb[identity] = pos.to_i
        end
      end
    rescue
      #No existing sincedb to load
      @logger.debug? && @logger.debug("_sincedb_open: error: #{@sincedb_path}: #{$!}")
    end
  end # def _sincedb_open

  private
  def _sincedb_write
    begin
      IO.write(@sincedb_path, serialize_sincedb, 0)
    rescue Errno::EACCES
      # probably no file handles free
      # maybe it will work next time
      @logger.debug? && @logger.debug("_sincedb_write: error: #{@sincedb_path}: #{$!}")
    end
  end # def _sincedb_write


  private
  def serialize_sincedb
    @sincedb.map do |identity, pos|
      [identity, pos].join(" ")
    end.join("\n") + "\n"
  end
end # class LogStash::Inputs::CloudWatch_Logs
