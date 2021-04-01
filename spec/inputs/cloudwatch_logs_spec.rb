# encoding: utf-8
require 'spec_helper'
require 'logstash/devutils/rspec/spec_helper'
require 'logstash/inputs/cloudwatch_logs'
require 'aws-sdk-resources'
require 'aws-sdk'

describe LogStash::Inputs::CloudWatch_Logs do
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
        'sincedb_path' => Dir.mktmpdir("rspec-")  + '/sincedb.txt'
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
        
        # given we set some times
        subject.send(:set_sincedb_value, *['group1', 'stream1', 2])
        subject.send(:set_sincedb_value, *['group1', 'stream2', 1])
        subject.send(:set_sincedb_value, *['group2', 'stream3', 3])

      
        # then these are the times we get when we look for one group
        start_time, stream_positions = subject.send(:get_sincedb_group_values, *['group1'])
        expect(start_time).to eq(1)
        expect(stream_positions).to eq({'group1:stream1' => 2, 'group1:stream2' => 1})        
      end

      it 'select the default time when asked' do
        subject.register
        
        # given we set some times
        subject.send(:set_sincedb_value, *['group1', 'stream1', 2])
        subject.send(:set_sincedb_value, *['group1', 'stream2', 1])
        subject.send(:set_sincedb_value, *['group2', 'stream3', 3])

        # then these are the times we get looking for a new group
        now_time = Time.now.getutc.to_i * 1000
        start_time, stream_positions = subject.send(:get_sincedb_group_values, *['groupXXX'])

        # the default time is now, for testing we can assume within a second is close enough
        ts_delta = now_time - start_time
        expect(ts_delta).to be < 1000
        # the stream positions should be empty
        expect(stream_positions).to eq({})        
      end     
      
      
    end
  end

  # describe '#find_log_groups with prefix true' do
  #   subject {LogStash::Inputs::CloudWatch_Logs.new(config.merge({'log_group_prefix' => true}))}
  #
  #   before(:each) {subject.register}
  #
  #   it 'should create list of prefixes' do
  #     expect_any_instance_of(Aws::CloudWatchLogs::Resource).to receive(:describe_log_groups).and_return({'log_groups' => [{'log_group_name' => '1'},{'log_group_name' => '2'}]})
  #     expect(subject.find_log_groups).to eq(['sample-log-group-1', 'sample-log-group-2'])
  #   end
  # end

end
