# encoding: utf-8
require 'logstash/devutils/rspec/spec_helper'
require 'logstash/inputs/cloudwatch_logs'
require 'aws-sdk-resources'
require 'aws-sdk'
require "logstash/timestamp"

describe LogStash::Inputs::CloudWatch_Logs do
  def parse_time(data)
    LogStash::Timestamp.at(data.to_i / 1000, (data.to_i % 1000) * 1000)
  end # def parse_time


  let(:config) {
    {
        'access_key_id' => '1234',
        'secret_access_key' => 'secret',
        'log_group' => ['sample-log-group'],
        'region' => 'us-east-1'
    }
  }

  before do
    Aws.config[:stub_responses] = true
    Thread.abort_on_exception = true
  end


  context 'when interrupting the plugin' do
    let(:config) {super.merge({'interval' => 5})}

    before do
      expect_any_instance_of(LogStash::Inputs::CloudWatch_Logs).to receive(:process_group).and_return(nil)
    end

    it_behaves_like 'an interruptible input plugin'
  end

  describe '#register' do
    context 'default config' do
      subject {LogStash::Inputs::CloudWatch_Logs.new(config)}

      it 'registers succesfully' do
        expect {subject.register}.to_not raise_error
      end
    end

    context 'start_position set to end' do
      subject {LogStash::Inputs::CloudWatch_Logs.new(config.merge({'start_position' => 'end'}))}

      it 'registers succesfully' do
        expect {subject.register}.to_not raise_error
      end
    end

    context 'start_position set to an integer' do
      subject {LogStash::Inputs::CloudWatch_Logs.new(config.merge({'start_position' => 100}))}

      it 'registers succesfully' do
        expect {subject.register}.to_not raise_error
      end
    end

    context 'start_position invalid' do
      subject {LogStash::Inputs::CloudWatch_Logs.new(config.merge({'start_position' => 'invalid start position'}))}

      it 'raises a configuration error' do
        expect {subject.register}.to raise_error(LogStash::ConfigurationError)
      end
    end
  end


  describe '#find_log_groups without prefix true' do
    context 'with an array in the config' do
      subject {LogStash::Inputs::CloudWatch_Logs.new(config)}

      it 'passes through configuration' do
        expect(subject.find_log_groups).to eq(['sample-log-group'])
      end
    end

    context 'with a single string in the log_group' do
      subject {LogStash::Inputs::CloudWatch_Logs.new(config.merge({'log_group' => 'sample-log-group-string'}))}

      it 'array-ifies the single string' do
        expect(subject.find_log_groups).to eq(['sample-log-group-string'])
      end
    end
  end

  
  describe '#process_log' do
    context 'with an array in the config' do      
      subject {LogStash::Inputs::CloudWatch_Logs.new(config.merge({
        'start_position' => 0,
        'sincedb_path' => Dir.mktmpdir("rspec-")  + '/sincedb.txt'
        }))}

      it 'check  default start time - beginning ' do
        # given the config is for beginning
        subject.instance_variable_set(:@start_position, 'beginning')

        # then the default time is epoch start 
        expect(subject.send(:get_default_start_time, *[])).to eq(0)
      end

      it 'check  default start time - end ' do
        # given the config is for end
        subject.instance_variable_set(:@start_position, 'end')

        # then the default time nearly now 
        now = DateTime.now.strftime('%Q').to_i
        expect(subject.send(:get_default_start_time, *[])).to be_within(100).of(now)
      end

      it 'check  default start time - start position ' do
        # given the config is for end
        subject.instance_variable_set(:@start_position, '86400')

        # then the default time nearly the expected  
        now = DateTime.now.strftime('%Q').to_i
        expected = now - 86400 * 1000
        expect(subject.send(:get_default_start_time, *[])).to be_within(100).of(expected)
      end      

      it 'process a log event - event is new' do
        subject.register
        event_tracker = subject.instance_variable_get(:@event_tracker)

        # given these times
        old_timestamp = DateTime.now.strftime('%Q').to_i
        new_timestamp = old_timestamp + 1000

        # given we know about this group and stream from an old record
        group = 'groupA'      
        # given we got the message for this group, for the known stream
        # and where the record is "new enough"
        log = Aws::CloudWatchLogs::Types::FilteredLogEvent.new()
        log.message = 'this be the verse'
        log.timestamp = old_timestamp
        log.ingestion_time = 123
        log.log_stream_name = 'streamX'
        log.event_id = 'event1'

        event_tracker.record_processed_event(group, log)

        # update this log to be a new event
        log.timestamp = new_timestamp
        log.event_id = 'event2'

        # when we send the log (assuming we have a queue)
        queue = []
        subject.instance_variable_set(:@queue, queue)

        subject.send(:process_log, *[log, group])

        # then a message was sent to the queue
        expect(queue.length).to eq(1)

        
        expect(queue[0].get('[@timestamp]')).to eq(LogStash::Timestamp.at(new_timestamp.to_i / 1000, (new_timestamp.to_i % 1000) * 1000))
        expect(queue[0].get('[message]')).to eq('this be the verse')
        expect(queue[0].get('[cloudwatch_logs][log_group]')).to eq('groupA')
        expect(queue[0].get('[cloudwatch_logs][log_stream]')).to eq('streamX')
        expect(queue[0].get('[cloudwatch_logs][ingestion_time]').to_iso8601).to eq('1970-01-01T00:00:00.123Z')

        # then the timestamp should have been updated
        start_time = event_tracker.min_time(group)
        # and the new start time the earliest record
        expect(start_time).to eq(old_timestamp)
      end
    
    

      it 'process a log event - event is old' do
        subject.register
        event_tracker = subject.instance_variable_get(:@event_tracker)

        # given these times
        old_timestamp = DateTime.now.strftime('%Q').to_i
        new_timestamp = old_timestamp + 1000


        puts("old_timestamp: #{old_timestamp}")
        puts("new_timestamp: #{new_timestamp}")


        # given we previously got the old record
        group = 'GroupA'
        log = Aws::CloudWatchLogs::Types::FilteredLogEvent.new()
        log.message = 'this be the verse'
        log.timestamp = new_timestamp
        log.ingestion_time = 123
        log.log_stream_name = 'streamX'
        log.event_id = 'eventA'
        event_tracker.record_processed_event(group, log)

        # given a new log message
        log.timestamp = old_timestamp
        log.event_id = 'eventB'


        # when we send the log (assuming we have a queue)
        queue = []
        subject.instance_variable_set(:@queue, queue)

        subject.send(:process_log, *[log, group])

        # then no message was sent to the queue
        expect(queue.length).to eq(0)

        # then the timestamp should not have been updated
        start_time = event_tracker.min_time(group)
        # and the new start time is 1 millisecond after the message time
        expect(start_time).to eq(new_timestamp)

      end    

      it 'process a log event - event has already been seen' do
        subject.register
        event_tracker = subject.instance_variable_get(:@event_tracker)

        # given these times
        old_timestamp = DateTime.now.strftime('%Q').to_i


        # given we previously got the old record
        group = 'GroupA'
        log = Aws::CloudWatchLogs::Types::FilteredLogEvent.new()
        log.message = 'this be the verse'
        log.timestamp = old_timestamp
        log.ingestion_time = 123
        log.log_stream_name = 'streamX'
        log.event_id = 'eventA'
        event_tracker.record_processed_event(group, log)


        # when we send the log (assuming we have a queue)
        queue = []
        subject.instance_variable_set(:@queue, queue)

        subject.send(:process_log, *[log, group])

        # then no message was sent to the queue
        expect(queue.length).to eq(0)

        # then the timestamp should not have been updated
        start_time = event_tracker.min_time(group)
        # and the new start time is 1 millisecond after the message time
        expect(start_time).to eq(old_timestamp)

      end          
    end

    
  end    

end
