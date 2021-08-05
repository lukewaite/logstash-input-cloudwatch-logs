# encoding: utf-8
require "fileutils"
require 'json'
java_import org.apache.logging.log4j.LogManager

# Tracks the last events for a group
# Contains only the last timestamp, and the events for that timestamp
class LastEvents    

  def initialize(timestamp = 0, events = [])
    @timestamp = timestamp
    @events = events
  end

  def timestamp
    @timestamp
  end

  def events
    @events
  end

  # Check the timestamp / event id
  # Returns true: If the timestamp/event is already in the model
  # Returns false: if the timestamp is not new, but the event is
  # Returns false: if the timestamp is new. This will cause the entire model to be flushed
  def check_event_already_processed(timestamp, event_id)
    if timestamp == @timestamp 
      if @events.include? event_id
        return true
      end 
      @events.push(event_id)
      return false
    else
      @timestamp = timestamp
      @events.clear()
      @events.push(event_id)
      return false
    end
  end
 

end

# Tracks the last events for each log stream
#
# In all cases log_event is one of the events from 
# https://docs.aws.amazon.com/sdk-for-ruby/v2/api/Aws/CloudWatchLogs/Client.html#filter_log_events-instance_method
class LastEventTracker
  include LogStash::Util::Loggable

  def initialize(path_to_data, prune_since_db_stream_minutes)
    @path_to_data = path_to_data
    @last_events_by_stream = Hash.new
    @logger = LogManager.getLogger(LastEventTracker)
    @prune_since_db_stream_minutes = prune_since_db_stream_minutes
  end

  # Given a log group and log event
  # - Return truthy if event was already processed, or falsy, otherwise
  # - Will record all the event ids with for the last timestamp   
  def check_event_already_processed(log_group, log_event)
    id = identify(log_group, log_event)
    if !@last_events_by_stream.key?(id)
      @last_events_by_stream[id] = LastEvents.new
    end

    return @last_events_by_stream[id].check_event_already_processed(log_event.timestamp, log_event.event_id)
  end

  # Get the last event ids for the given group/event
  def get(log_group, log_event)
    id = identify(log_group, log_event)
    return @last_events_by_stream[id]
  end



  # saves the data model to json 
  def save() 
    # build the json model
    data = {}
    @last_events_by_stream.each do |k,v| 
      data[k] = {
        "timestamp" => @last_events_by_stream[k].timestamp,
        "events" => @last_events_by_stream[k].events
      }
    end  
    
    
    # save it
    begin      
      File.write(@path_to_data, data.to_json)
    rescue Errno::EACCES
      # probably no file handles free
      # maybe it will work next time
      @logger.debug? && @logger.debug("Failed to write to: #{@path_to_data}: #{$!}")
    end    
  end

  def load
    @logger.debug("Loading config from: #{@path_to_data}")  

    begin
      @last_events_by_stream.clear
      if File.file?(@path_to_data)        
        data_hash = JSON.parse(File.read(@path_to_data))
        data_hash.each do |k, v|
          @last_events_by_stream[k] = LastEvents.new(v['timestamp'], v['events'])
        end
      end
    rescue 
      # if we can't read the file, we just assume it's broken, and we'll ignore it
      @logger.debug("Failed to read: #{@path_to_data}: #{$!}")  
    end
  end 

  # Create a key to uniquely identify the group/stream for the log event
  private
  def identify(log_group, log_event)
    return log_group + '.' + log_event.log_stream_name
  end

  private 
  def purge_old()
    # find the max time
    max_time = 0
    @last_events_by_stream.each do |k, v|
      if max_time < v['timestamp']
        max_time = v['timestamp']
      end
    end

    # build the lookback time
    purge_before = (max_time - (60 * 1000 * @prune_since_db_stream_minutes))  

    # prune the old stream data
    @last_events_by_stream.clone.each do |k, v|
      if v['timestamp'] < purge_before
        @sincedb.delete(k)
      end
    end    
  end  
  
end 
