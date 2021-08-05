# encoding: utf-8
require 'logstash/devutils/rspec/spec_helper'
require 'logstash/inputs/last_event_tracker'
require 'aws-sdk-resources'
require 'aws-sdk'

describe LastEventTracker do

  describe "test add events" do
    
    it "add single event"   do
      tracker = LastEventTracker.new(Dir.mktmpdir("rspec-")  + '/sincedb.txt', 15)

      # given a new log event
      log = Aws::CloudWatchLogs::Types::FilteredLogEvent.new()
      log.message = 'this be the verse'
      log.timestamp = 1
      log.log_stream_name = 'streamX'
      log.event_id = 'event1'

      # when we push the first event, then it will not have been processed
      expect(tracker.check_event_already_processed('group', log)).to be_falsey
      
      # and it will be added 
      expect(tracker.get('group', log).events).to contain_exactly(log.event_id)
      expect(tracker.get('group', log).timestamp).to eq(log.timestamp)                 
    end


    
    it "add multiple events in the same time period"   do
      tracker = LastEventTracker.new(Dir.mktmpdir("rspec-")  + '/sincedb.txt', 15)

      # given multiple log events in the same time period
      log1 = Aws::CloudWatchLogs::Types::FilteredLogEvent.new()
      log1.timestamp = 1
      log1.log_stream_name = 'streamX'
      log1.event_id = 'event1'

      log2 = Aws::CloudWatchLogs::Types::FilteredLogEvent.new()
      log2.timestamp = log1.timestamp
      log2.log_stream_name = 'streamX'
      log2.event_id = 'event2'
      
      log3 = Aws::CloudWatchLogs::Types::FilteredLogEvent.new()
      log3.timestamp = log1.timestamp
      log3.log_stream_name = 'streamX'
      log3.event_id = 'event3'      

      # when we push the first event, then it will not have been processed
      expect(tracker.check_event_already_processed('group', log1)).to be_falsey
      expect(tracker.check_event_already_processed('group', log2)).to be_falsey
      expect(tracker.check_event_already_processed('group', log3)).to be_falsey
      
      # and they will be added 
      expect(tracker.get('group', log3).events).to contain_exactly(log1.event_id, log2.event_id, log3.event_id)
      expect(tracker.get('group', log3).timestamp).to eq(log1.timestamp)                 
    end

    it "add duplicate event"   do
      tracker = LastEventTracker.new(Dir.mktmpdir("rspec-")  + '/sincedb.txt', 15)

      # given a new log event
      log = Aws::CloudWatchLogs::Types::FilteredLogEvent.new()
      log.message = 'this be the verse'
      log.timestamp = 1
      log.log_stream_name = 'streamX'
      log.event_id = 'event1'

      # when we push the first event, then it will not have been processed
      expect(tracker.check_event_already_processed('group', log)).to be_falsey
      expect(tracker.check_event_already_processed('group', log)).to be_truthy
      
      # and it will be added 
      expect(tracker.get('group', log).events).to contain_exactly(log.event_id)
      expect(tracker.get('group', log).timestamp).to eq(log.timestamp)    
      
      tracker.save()
    end


    it "add multiple events in a new time period"   do
      tracker = LastEventTracker.new(Dir.mktmpdir("rspec-")  + '/sincedb.txt', 15)

      # given multiple log events in the same time period
      log1 = Aws::CloudWatchLogs::Types::FilteredLogEvent.new()
      log1.timestamp = 1
      log1.log_stream_name = 'streamX'
      log1.event_id = 'event1'

      log2 = Aws::CloudWatchLogs::Types::FilteredLogEvent.new()
      log2.timestamp = log1.timestamp
      log2.log_stream_name = 'streamX'
      log2.event_id = 'event2'
      
      log3 = Aws::CloudWatchLogs::Types::FilteredLogEvent.new()
      log3.timestamp = log1.timestamp + 1
      log3.log_stream_name = 'streamX'
      log3.event_id = 'event3'      

      # when we push the first event, then it will not have been processed
      expect(tracker.check_event_already_processed('group', log1)).to be_falsey
      expect(tracker.check_event_already_processed('group', log2)).to be_falsey
      expect(tracker.check_event_already_processed('group', log3)).to be_falsey
      
      # and they will be added 
      expect(tracker.get('group', log3).events).to contain_exactly(log3.event_id)
      expect(tracker.get('group', log3).timestamp).to eq(log3.timestamp)                 
    end


    it "test file handling"   do
      pathToFile = Dir.mktmpdir("rspec-")  + '/sincedb.txt'

      puts ("Testing with #{pathToFile}")
      File.open(pathToFile, 'w') { |file| file.write('{"group.streamX": {"timestamp": 1, "events" : ["event1"]}}') }

      tracker = LastEventTracker.new(pathToFile, 15)

      # given multiple log events in the different streams
      log1 = Aws::CloudWatchLogs::Types::FilteredLogEvent.new()
      log1.timestamp = 1
      log1.log_stream_name = 'streamX'
      log1.event_id = 'event1'

      log2 = Aws::CloudWatchLogs::Types::FilteredLogEvent.new()
      log2.timestamp = log1.timestamp
      log2.log_stream_name = 'streamY'
      log2.event_id = 'event2'
      
      log3 = Aws::CloudWatchLogs::Types::FilteredLogEvent.new()      
      log3.timestamp = log1.timestamp
      log3.log_stream_name = 'streamZ'
      log3.event_id = 'event3'

      tracker.check_event_already_processed('groupA', log1)
      tracker.check_event_already_processed('groupA', log2)
      tracker.check_event_already_processed('groupB', log3)

      # and the model is saved
      tracker.save()
      
      # and we create a new tracker and reload the data from the saved file
      tracker = LastEventTracker.new(pathToFile, 15)
      tracker.load()

      expect(tracker.get('groupA', log1).events).to contain_exactly(log1.event_id)
      expect(tracker.get('groupA', log1).timestamp).to eq(1)                 

      expect(tracker.get('groupA', log2).events).to contain_exactly(log2.event_id)
      expect(tracker.get('groupA', log2).timestamp).to eq(1)        

      expect(tracker.get('groupB', log3).events).to contain_exactly(log3.event_id)
      expect(tracker.get('groupB', log3).timestamp).to eq(1)        
    end

    it "test file handling on first run"   do

      # given a tracker to a file that doesn't exist
      tracker = LastEventTracker.new("not a real path", 15)

      # load it
      tracker.load()

      # given a new log event
      log = Aws::CloudWatchLogs::Types::FilteredLogEvent.new()
      log.message = 'this be the verse'
      log.timestamp = 1
      log.log_stream_name = 'streamX'
      log.event_id = 'event1'

      # then the log event should be there
      expect(tracker.get('group', log)).to be_falsey
      # and it should work
      tracker.check_event_already_processed('group', log)
      expect(tracker.get('group', log).events).to contain_exactly(log.event_id)

    end

  end

end
