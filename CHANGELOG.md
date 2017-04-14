# Release Notes for `logstash-input-cloudwatch_logs`

## v0.10.0 (2017-04-01)

## Added
* `log_group_prefix` parameter supporting ingesting a set of log groups matching a prefix ([#9](https://github.com/lukewaite/logstash-input-cloudwatch-logs/pull/9))

## Fixed
* Step back when throttled by Amazon ([#9](https://github.com/lukewaite/logstash-input-cloudwatch-logs/pull/9))

## v0.9.4 (2017-03-31)

## Fixed
* Fix autoloading of aws-sdk ([#15](https://github.com/lukewaite/logstash-input-cloudwatch-logs/pull/15))

## v0.9.3 (2016-12-22)

## Added
* Support for Logstash version 5.x ([#6e7cc5d](https://github.com/lukewaite/logstash-input-cloudwatch-logs/commit/6e7cc5decdcd7a8d8528d42a7b040b1d2f3a3490))

## v0.9.2 (2016-07-21)

## Added
* Initial publish to RubyGems

## v0.9.1 (2016-07-19)

### Added
* Support for Logstash version 2.x ([#8824ae9](https://github.com/lukewaite/logstash-input-cloudwatch-logs/commit/8824ae9899fa0e1d0a627796479824bc6f5c39b2))

## v0.9.0 (2015-07-06)

### Initial Release
* This is the initial release of the input