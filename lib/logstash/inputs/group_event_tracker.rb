java_import org.apache.logging.log4j.LogManager

#
# Tracks if an event is new and stores this event.
# The process is to maintain a window of N minutes in which every event is stored by 
# millisecond, and stream/event id. 
#
# Events are "new" if they are after the beginning of this window, and if they are not 
# already recorded in the window (as identified by the stream/event id)
#
# The window is purged once N minutes of events are processed (note, this is by
# log event time). It is, therefore, and assumption that log events are processed
# in as close to time order as possible (at least, within the N minute window).
# This is an automatic consequence of the AWS filter_log_events method.
#
# In all cases log_event is one of the events from 
# https://docs.aws.amazon.com/sdk-for-ruby/v2/api/Aws/CloudWatchLogs/Client.html#filter_log_events-instance_method
class LogEventTracker
    include LogStash::Util::Loggable

    def initialize(path_to_data, prune_since_db_stream_minutes)
        @logger = LogManager.getLogger(LogEventTracker)
        @path_to_data = path_to_data
        @prune_since_db_stream_minutes = prune_since_db_stream_minutes    

        # maps groups to GroupEventTracker
        @group_trackers = {}
    end

    # returns true if the event hasn't been processed yet
    def is_new_event(group, log_event)
        return ensure_group(group).is_new_event(log_event)
    end

    # records the new event in the log
    def record_processed_event(group, log_event)
        ensure_group(group).record_processed_event(log_event)        
    end

    # wipe any events older than the prune time (using the last records 
    # in the window as the end time)
    def purge (group)
        ensure_group(group).purge
    end

    def min_time (group, default_time = nil)
        return ensure_group(group).min_time(default_time)
    end

    def get_or_set_min_time(group, default_time = nil)
        return ensure_group(group).get_or_set_min_time(default_time)
    end

    def save()
        # build the json model
        save_data = {}
        @group_trackers.each do |k,v| 
            save_data[k] = v.to_save_model
        end  

        # save it
        begin      
            File.write(@path_to_data, save_data.to_json)
            rescue Errno::EACCES
            # probably no file handles free
            # maybe it will work next time
            @logger.debug? && @logger.debug("Failed to write to: #{@path_to_data}: #{$!}")
        end               
    end

    def load()
        # load the file into json
        begin
            load_new_format
        rescue JSON::ParserError
            load_old_format
        rescue 
            # if we can't read the file, we just assume it's broken, and we'll ignore it
            @logger.debug("Failed to read: #{@path_to_data}: #{$!}")  
        end        
    end

    private
    def load_new_format()
        @group_trackers.clear
        if File.file?(@path_to_data)        
          data_hash = JSON.parse(File.read(@path_to_data))
          data_hash.each do |k, v|
            group_entry = ensure_group(k).from_save_model(v)
          end
        end        
    end

    private
    def load_old_format()
        # group1:stream2 123
        # group1 456        
        @group_trackers.clear
        File.open(@path_to_data) do |db|
            db.each do |line|
                identity, pos = line.split(" ", 2)
                if identity.include? ":"
                    identity = identity[0..identity.index(":") - 1]
                end
                ensure_group(identity).update_ranges(pos.to_i)
                ensure_group(identity).set_to_tail
    
            end
        end

    end

    private 
    def ensure_group(group)
        if !@group_trackers.key?(group) 
            @group_trackers[group] = GroupEventTracker.new (@prune_since_db_stream_minutes)
        end        
        return @group_trackers[group]
    end
    

end     
# Maintains the event window at the level of a single group
class GroupEventTracker
    include LogStash::Util::Loggable

    def initialize( prune_since_db_stream_minutes)
        @logger = LogManager.getLogger(GroupEventTracker)

        @prune_since_db_stream_minutes = prune_since_db_stream_minutes

        @min_time = nil
        @max_time = nil  
        
        # maps a log event timestamp (in millis) to the events in that millisecond
        @events_by_ms = {}
    end

    def min_time(default_time = nil)
        if @min_time.nil?
            return default_time
        end

        return @min_time
    end

    def get_or_set_min_time (default_time = nil)
        
        if @min_time.nil?
            @min_time = default_time
        end

        return @min_time
    end    

    # returns true if the event hasn't been processed yet
    def is_new_event(log_event)       
        # we've seen no records at all
        if @min_time.nil?
            return true
        # the record is too old
        elsif log_event.timestamp < @min_time 
            return false
        # so either the timestamp is new or the event is
        else 
            if !@events_by_ms.key?(log_event.timestamp) 
                return true
            else
                return !@events_by_ms[log_event.timestamp].include?( identify(log_event))              
            end
        end
    end

    # records the new event in the log
    def record_processed_event(log_event)
        # update the min/max times
        update_ranges(log_event.timestamp)
        
        # store the event in the ms part of the process window
        if !@events_by_ms.key?(log_event.timestamp) 
            @events_by_ms[log_event.timestamp] = []
        end
        @events_by_ms[log_event.timestamp].push(identify(log_event))
    end    

    def purge ()
        # if we've gotten no data, there's nothing top do
        if @max_time.nil?
            return
        end

        # if our window is all after the purge time we have nothing to do
        purge_before = (@max_time - (60 * 1000 * @prune_since_db_stream_minutes))  
        if @min_time > purge_before
            return
        else
            # otherwise reset the min time and purge everything before it
            @min_time = purge_before
            @events_by_ms.clone.each do |k, v|
                if k < purge_before
                    @events_by_ms.delete(k)
                end
            end
        end           
    end    

    def identify(log_event)
        return "#{log_event.log_stream_name}:#{log_event.event_id}"
    end

    def to_save_model()
        save_data = {}
        @events_by_ms.each do |k,v| 
            save_data[k] = v
        end  
        return save_data
    end  

    def from_save_model(json_model) 
        @events_by_ms.clear()
        json_model.each do |k, v|
            ts = k.to_i            
            update_ranges(ts)
            @events_by_ms[ts] = v
        end
    end
     
    def update_ranges(timestamp)
        if @min_time.nil? || @min_time > timestamp
            @min_time = timestamp
        end
        if @max_time.nil? || @max_time < timestamp
            @max_time = timestamp
        end    
    end

    def set_to_tail
        if !@max_time.nil? 
            @min_time = @max_time
            @events_by_ms.clear
        end
    end

end