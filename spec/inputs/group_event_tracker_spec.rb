# encoding: utf-8
require 'logstash/devutils/rspec/spec_helper'
require 'logstash/inputs/group_event_tracker'
require 'aws-sdk-resources'
require 'aws-sdk'

describe LogEventTracker do

  describe "test check events" do
    
    it "add new event to empty model"   do
      tracker = LogEventTracker.new(Dir.mktmpdir("rspec-")  + '/sincedb.txt', 15)

      # given a new log event
      log = Aws::CloudWatchLogs::Types::FilteredLogEvent.new()
      log.message = 'this be the verse'
      log.timestamp = 1
      log.log_stream_name = 'streamX'
      log.event_id = 'event1'

      # when we push the first event, then it will not have been processed
      expect(tracker.is_new_event('group', log)).to be_truthy

      # now when we add it it will processed
      tracker.record_processed_event('group', log)
      expect(tracker.is_new_event('group', log)).to be_falsey
    end  
    
    it "add multiple events"   do
      tracker = LogEventTracker.new(Dir.mktmpdir("rspec-")  + '/sincedb.txt', 15)

      # given a new log event in some group
      group = 'group'      
      log = Aws::CloudWatchLogs::Types::FilteredLogEvent.new()
      log.timestamp = 1
      log.log_stream_name = 'streamX'
      log.event_id = 'event1'

      logSameTime = Aws::CloudWatchLogs::Types::FilteredLogEvent.new()
      logSameTime.timestamp = 1
      logSameTime.log_stream_name = log.log_stream_name
      logSameTime.event_id = 'event2'      

      logSameTimeDifferentStream = Aws::CloudWatchLogs::Types::FilteredLogEvent.new()
      logSameTimeDifferentStream.timestamp = 1
      logSameTimeDifferentStream.log_stream_name = 'streamY'
      logSameTimeDifferentStream.event_id = 'event2'         

      # given the first log is in place
      tracker.record_processed_event(group, log)

      # then it should already be in place
      expect(tracker.is_new_event(group, log)).to be_falsey
      # but others in the same time, but different ids or streams should be new
      expect(tracker.is_new_event(group, logSameTime)).to be_truthy
      expect(tracker.is_new_event(group, logSameTimeDifferentStream)).to be_truthy

    end   

    it "check purge works"   do
      purge_minutes = 3
      tracker = LogEventTracker.new(Dir.mktmpdir("rspec-")  + '/sincedb.txt', purge_minutes)

      # given a new log event
      too_old = Aws::CloudWatchLogs::Types::FilteredLogEvent.new()
      too_old.timestamp = 1
      too_old.log_stream_name = 'streamX'
      too_old.event_id = 'event1'

      not_too_old = Aws::CloudWatchLogs::Types::FilteredLogEvent.new()
      not_too_old.timestamp = too_old.timestamp + 60 * 1000
      not_too_old.log_stream_name = 'streamX'
      not_too_old.event_id = 'event2' 

      now = Aws::CloudWatchLogs::Types::FilteredLogEvent.new()
      now.timestamp = too_old.timestamp + 60 * 1000 * purge_minutes + 1
      now.log_stream_name = 'streamX'
      now.event_id = 'event3'       

      # push in the three messages
      group = 'group'      
      tracker.record_processed_event(group, too_old)
      tracker.record_processed_event(group, not_too_old)
      tracker.record_processed_event(group, now)
      
      # purge 
      tracker.purge(group)

      # now, get the group data out and check it
      group_tracker = tracker.send(:ensure_group, *[group])
      group_data = group_tracker.instance_variable_get(:@events_by_ms)
      
      # then 
      expect(group_data.key?(too_old.timestamp)).to be_falsey
      expect(group_data.key?(not_too_old.timestamp)).to be_truthy
      expect(group_data.key?(now.timestamp)).to be_truthy
    end      

    it "check save data"   do
      # given an existing file at this location
      purge_minutes = 3
      pathToFile = Dir.mktmpdir("rspec-")  + '/sincedb.txt'      
      puts("pathToFile => #{pathToFile}")
      tracker = LogEventTracker.new(pathToFile, purge_minutes)

      # write in some logs
      log = Aws::CloudWatchLogs::Types::FilteredLogEvent.new()
      log.timestamp = 1
      log.log_stream_name = 'streamX'
      log.event_id = 'event1'
      tracker.record_processed_event("groupA", log)
      tracker.record_processed_event("groupB", log)

      log.timestamp = 2
      log.log_stream_name = 'streamY'
      log.event_id = 'event2'
      tracker.record_processed_event("groupA", log)


      # save them
      tracker.save

      # create a new tracker and reload the file
      tracker = LogEventTracker.new(pathToFile, purge_minutes)
      tracker.load

      # now, get the group data out and check it
      group_tracker = tracker.send(:ensure_group, *['groupA'])
      group_data = group_tracker.instance_variable_get(:@events_by_ms)

      expect(group_data[1]).to contain_exactly("streamX:event1")
      expect(group_data[2]).to contain_exactly("streamY:event2")

      expect(group_tracker.instance_variable_get(:@min_time)).to eq(1)
      expect(group_tracker.instance_variable_get(:@max_time)).to eq(2)

      # group b data
      group_tracker = tracker.send(:ensure_group, *['groupB'])
      group_data = group_tracker.instance_variable_get(:@events_by_ms)      

      expect(group_data[1]).to contain_exactly("streamX:event1")

      expect(group_tracker.instance_variable_get(:@min_time)).to eq(1)
      expect(group_tracker.instance_variable_get(:@max_time)).to eq(1)            

    end    

    it "check old save data format" do
      # given an existing file at this location
      purge_minutes = 3
      pathToFile = Dir.mktmpdir("rspec-")  + '/sincedb.txt'      
      puts("pathToFile => #{pathToFile}")
      tracker = LogEventTracker.new(pathToFile, purge_minutes)

      # given a file in the new "group:stream position" format        
      File.open(pathToFile, "w") { |f| 
        f.write "group1:stream1 1\n"
        f.write "group1:stream2 2\n"
        f.write "group1 3\n"
        f.write "group2 4\n"
        f.write "group2:stream1 5\n"
      }

      # load the tracker
      tracker.load        

      # check the groups - when we load the old file we use the end as the min/max
      group_tracker = tracker.send(:ensure_group, *['group1'])
      expect(group_tracker.instance_variable_get(:@min_time)).to eq(3)
      expect(group_tracker.instance_variable_get(:@max_time)).to eq(3)

      group_tracker = tracker.send(:ensure_group, *['group2'])
      expect(group_tracker.instance_variable_get(:@min_time)).to eq(5)
      expect(group_tracker.instance_variable_get(:@max_time)).to eq(5)      
    

    end
    
   

  end

end
