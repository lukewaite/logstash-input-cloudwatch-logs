# Architecture

## Complexities in the source data that can cause query problems

The data source here is cloudwatch, whose `filter_log_events` method
returns log data for a single log group, possibly split into multiple
streams. It will attempt to interpolate these so they are in event
time order, but can't guarantee that.

In a high volume situation this results in two issues that can cause
problems in queries:

- Log messages in a group's query result may appear out of order; 
  although they will be in order as far as the stream is concerned.
- You may get multiple messages with the same timestamp


These cause a problem because there's no query model for tailing 
a log stream (it's possible, but only with architectural changes 
in your deployment) so the only way to get data is to record when
the last message happened and search again from there. 

The above issues impact this as follows:

- *Out of order messages* - when to set the last event time
    to avoid missing messages?
- *Multiple messages with the same timestamp* - if you have 
    2 messages in the original stream with the same timestamp,
    and your last query returned the first one, how do you query
    to get the second, without reprocessing the first one?

## Resolution using a log event tracking window

This was resolved in the [LogEventTracker](lib/logstash/inputs/group_event_tracker.rb)
by maintaining a record of a window of log events, storing every event in that period. 

**NOTE:** all times are derived from the log event timestamp and
not the current actual timestamp. They are accurate to the millisecond.

The model is per log_group:

- `min_time`: the earliest time for which we have a log event for this group
- `max_time`: the latest time for which we have a log event for this group
- `map[log_event_time] -> set[events]`: a record of all the events
    for this group in the log window.

In effect, we're keeping track of all the events we've seen in 
the window (e.g. a 15 minute period). Once we get more than, say,
15 minutes worth of events, we start dropping the older events. 

The window will tell you if a record is _"new"_ if:

- It's identified as an event we've never seen,  where an event is identified 
    as unique using its `stream` name and its `eventId`
- It's for a millisecond on or after the min_time.


The process for querying the data for some group is:

```#ruby
    # Get the earliest time for which we've seen any data    
    start_time = window.get_min_time(group)

    # This might contain events we've already processed
    events =  filter_log_events (group, start_time)

    # Loop through the events, skipping any we've already 
    # seen, and processing the rest
    events.each do |event|
        if !window.have_we_seen_this_before(group, event)
            process_event(group, event)
        end
    end

    # Once we've finished the search, purge any events that are too
    # old (e.g. more than 15 minutes older than the maximum timestamp)
    # and then save the data to a file so it's there if we restart
    window.purge_events_too_old(group)
    window.save_to_file(group)
```

In experiments I've found a 15 minute window avoids any missed records. In our
use cases, however, we've been routing through an aggregator that holds back
data for a few minutes to make sure it has enough data to push out, so you
can probably reduce this window to suit your own needs.

