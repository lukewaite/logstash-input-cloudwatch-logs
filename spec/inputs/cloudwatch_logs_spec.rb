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

  describe '#validate new stream db model' do
    context 'simple empty model' do
      
      subject {LogStash::Inputs::CloudWatch_Logs.new(config.merge({
        'start_position' => 0,
        'sincedb_path' => Dir.mktmpdir("rspec-")  + '/sincedb.txt',
        'prune_since_db_stream_minutes' => 2 * 60
        }))}
    
      it 'handle old files' do  
        subject.register

        puts('Testing with file: ' + subject.sincedb_path)
        # given a file in the old "group position" format        
        File.open(subject.sincedb_path, "w") { |f| 
          f.write "group1 1\n"
          f.write "group2 2\n"
        }

        # load the file
        subject.send(:_sincedb_open)

        # confirm we still get the group position (this is our fallback)
        start_time, stream_positions = subject.send(:get_sincedb_group_values, *['group1'])
        expect(start_time).to eq(1)
        expect(stream_positions).to eq({})  

        # confirm we still get the group position (this is our fallback)
        start_time, stream_positions = subject.send(:get_sincedb_group_values, *['group2'])
        expect(start_time).to eq(2)
        expect(stream_positions).to eq({})          
      end

      it 'handle new files' do  
        subject.register

        # given a file in the new "group:stream position" format        
        File.open(subject.sincedb_path, "w") { |f| 
          f.write "group1:stream1 2\n"
          f.write "group1:stream2 1\n"
          f.write "group2:stream1 3\n"
        }

        # load the file
        subject.send(:_sincedb_open)

        # confirm we still get the group position (this is our fallback)
        start_time, stream_positions = subject.send(:get_sincedb_group_values, *['group1'])
        expect(start_time).to eq(1)
        expect(stream_positions).to eq({"group1:stream1"=>2, "group1:stream2"=>1})  

        # confirm we still get the group position (this is our fallback)
        start_time, stream_positions = subject.send(:get_sincedb_group_values, *['group2'])
        expect(start_time).to eq(3)
        expect(stream_positions).to eq({"group2:stream1"=>3})          
      end      

      it 'select the correct group when asked' do
        subject.register
        
        t1 = DateTime.now.strftime('%Q').to_i
        t2 = t1 + 1000
        t3 = t1 + 2000
        
        puts("t1: #{t1}")

        # given we set some times
        subject.send(:set_sincedb_value, *['group1', 'stream1', t2])
        subject.send(:set_sincedb_value, *['group1', 'stream2', t1])
        subject.send(:set_sincedb_value, *['group2', 'stream3', t3])

      
        # then these are the times we get when we look for one group
        start_time, stream_positions = subject.send(:get_sincedb_group_values, *['group1'])
        expect(start_time).to eq(t1)
        expect(stream_positions).to eq({'group1:stream1' => t2, 'group1:stream2' => t1})        
      end

      it 'select the default time when asked' do
        subject.register
        t1 = DateTime.now.strftime('%Q').to_i 
        t2 = t1 + 1000
        t3 = t1 + 2000        
        # given we set some times
        subject.send(:set_sincedb_value, *['group1', 'stream1', t2])
        subject.send(:set_sincedb_value, *['group1', 'stream2', t1])
        subject.send(:set_sincedb_value, *['group2', 'stream3', t3])

        # then these are the times we get looking for a new group
        now_time = DateTime.now.strftime('%Q').to_i 
        start_time, stream_positions = subject.send(:get_sincedb_group_values, *['groupXXX'])

        # the default time is now, for testing we can assume within a second is close enough
        ts_delta = now_time - start_time
        expect(ts_delta).to be < 1000
        # the stream positions should be empty
        expect(stream_positions).to eq({})        
      end     
      

      it 'purge old stream data - general' do  
        subject.register

        puts('Testing with file: ' + subject.sincedb_path)


        now = DateTime.now.strftime('%Q').to_i 
        old = now - 3600 * 1000
        too_old = now - 2 * 3600  * 1000

        puts ("now: #{now} #{parse_time(now)}")
        puts ("old: #{old} #{parse_time(old)}")
        puts ("too_old: #{too_old}  #{parse_time(too_old)}")

        # given a file in the new "group:stream position" format        
        File.open(subject.sincedb_path, "w") { |f| 
          f.write "group1:stream1 #{old}\n"
          f.write "group1:stream2 #{too_old}\n"
          f.write "group2:stream1 #{too_old}\n"
        }

        # load the file
        subject.send(:_sincedb_open)

        # confirm our test data is correct
        start_time, stream_positions = subject.send(:get_sincedb_group_values, *['group1'])
        expect(start_time).to eq(too_old)
        expect(stream_positions).to eq({
          'group1:stream1' => old,
          'group1:stream2' => too_old
        })  

        start_time, stream_positions = subject.send(:get_sincedb_group_values, *['group2'])
        expect(start_time).to eq(too_old)
        expect(stream_positions).to eq({
          'group2:stream1' => too_old
        })    
        
        # now, write to the stream - this will purge old data in this group
        subject.send(:set_sincedb_value, *['group2', 'stream2', now])
        subject.send(:prune_since_db_stream, *['group2'])


        # and confirm the purge happened
        # group 1 wasn't updated, so it stays unpurged
        start_time, stream_positions = subject.send(:get_sincedb_group_values, *['group1'])
        expect(start_time).to eq(too_old)
        expect(stream_positions).to eq({
          'group1:stream1' => old,
          'group1:stream2' => too_old
        })  

        # group2 will have been updated and old data purged
        start_time, stream_positions = subject.send(:get_sincedb_group_values, *['group2'])
        expect(start_time).to eq(now)
        expect(stream_positions).to eq({
          'group2:stream2' => now
        })          
      end     
            
            
      it 'purge old stream data - check group reset ok' do  
        subject.register

        puts('Testing with file: ' + subject.sincedb_path)


        now = DateTime.now.strftime('%Q').to_i 
        recent = now - 1800 * 1000
        old = now - 3600 * 1000
        too_old = now - 2 * 3600  * 1000

        puts ("now:     #{now} #{parse_time(now)}")
        puts ("recent:  #{recent} #{parse_time(recent)}")
        puts ("old:     #{old} #{parse_time(old)}")
        puts ("too_old: #{too_old} #{parse_time(too_old)}")


        # given a file in the new "group:stream position" format        
        File.open(subject.sincedb_path, "w") { |f| 
          f.write "group1:stream1 #{too_old}\n"
          f.write "group1:stream2 #{too_old}\n"
          f.write "group1 #{too_old}\n"
          f.write "group2 #{too_old}\n"
          f.write "group2:stream1 #{too_old}\n"
        }

        # load the file
        subject.send(:_sincedb_open)

        # given we processed data for now
        subject.send(:set_sincedb_value, *['group1', 'stream1', too_old])
        subject.send(:set_sincedb_value, *['group2', 'stream1', now])

        # given a purge
        subject.send(:prune_since_db_stream, *['group1'])
        subject.send(:prune_since_db_stream, *['group2'])

        puts ("@sincedb #{subject.instance_eval {@sincedb}}")
        # This won't change
        expect(subject.instance_eval {@sincedb['group1']}).to eq(too_old)

        # The latest date in this group is 2 hours after "too old" so 
        # we update it
        expect(subject.instance_eval {@sincedb['group2']}).to eq(now)
  
      end           
      
    end
  end

  describe '#process_log' do
    context 'with an array in the config' do
      subject {LogStash::Inputs::CloudWatch_Logs.new(config.merge({
        'start_position' => 0,
        'sincedb_path' => Dir.mktmpdir("rspec-")  + '/sincedb.txt'
        }))}

      it 'process a log event - event is new' do
        subject.register

        # given these times
        old_timestamp = DateTime.now.strftime('%Q').to_i
        new_timestamp = old_timestamp + 1000

        # given we know about this group and stream
        group = 'groupA'      
        subject.send(:set_sincedb_value, *['groupA', 'streamX', old_timestamp])
        start_time, stream_positions = subject.send(:get_sincedb_group_values, *[group])


        # given we got the message for this group, for the known stream
        # and where the record is "new enough"
        log = Aws::CloudWatchLogs::Types::FilteredLogEvent.new()
        log.message = 'this be the verse'
        log.timestamp = new_timestamp
        log.ingestion_time = 123
        log.log_stream_name = 'streamX'

        # when we send the log (assuming we have a queue)
        queue = []
        subject.instance_variable_set(:@queue, queue)

        subject.send(:process_log, *[log, group, stream_positions])

        # then a message was sent to the queue
        expect(queue.length).to eq(1)

        
        expect(queue[0].get('[@timestamp]')).to eq(LogStash::Timestamp.at(new_timestamp.to_i / 1000, (new_timestamp.to_i % 1000) * 1000))
        expect(queue[0].get('[message]')).to eq('this be the verse')
        expect(queue[0].get('[cloudwatch_logs][log_group]')).to eq('groupA')
        expect(queue[0].get('[cloudwatch_logs][log_stream]')).to eq('streamX')
        expect(queue[0].get('[cloudwatch_logs][ingestion_time]').to_iso8601).to eq('1970-01-01T00:00:00.123Z')

        # then the timestamp should have been updated
        start_time, stream_positions = subject.send(:get_sincedb_group_values, *[group])
        # and the new start time is 1 millisecond after the message time
        expect(start_time).to eq(new_timestamp + 1)
        expect(stream_positions).to eq({group + ':streamX' => new_timestamp + 1})   

      end
    
    

      it 'process a log event - event is old' do
        subject.register

        # given these times
        old_timestamp = DateTime.now.strftime('%Q').to_i
        new_timestamp = old_timestamp - 1000

        # given we know about this group and stream
        group = 'groupA'      
        subject.send(:set_sincedb_value, *[group, 'streamX', old_timestamp])
        start_time, stream_positions = subject.send(:get_sincedb_group_values, *[group])

        

        # given we got the message for this group, for the known stream
        # and where the record is "new enough"
        log = Aws::CloudWatchLogs::Types::FilteredLogEvent.new()
        log.message = 'this be the verse'
        log.timestamp = new_timestamp
        log.ingestion_time = 123
        log.log_stream_name = 'streamX'

        # when we send the log (assuming we have a queue)
        queue = []
        subject.instance_variable_set(:@queue, queue)

        subject.send(:process_log, *[log, group, stream_positions])

        # then no message was sent to the queue
        expect(queue.length).to eq(0)

        # then the timestamp should not have been updated
        start_time, stream_positions = subject.send(:get_sincedb_group_values, *[group])
        # and the new start time is 1 millisecond after the message time
        expect(start_time).to eq(old_timestamp)
        expect(stream_positions).to eq({group + ':streamX' => old_timestamp})   
      end    
    end
  end    

end
