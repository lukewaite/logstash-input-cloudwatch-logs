# Release Notes for `logstash-input-cloudwatch_logs`

## v1.0.0 Pre-Release (2017-06-24)
* BREAKING CHANGE: `log_group` must now be an array, adds support for specifying multiple groups or prefixes (Fixes [#13](https://github.com/lukewaite/logstash-input-cloudwatch-logs/issues/13))
* Refactored ingestion, fixes multiple memory leaks (Fixes [#24](https://github.com/lukewaite/logstash-input-cloudwatch-logs/issues/4))
* Pull only log_events since last ingestion (Fixes [#10](https://github.com/lukewaite/logstash-input-cloudwatch-logs/issues/10))
* Incrementally write to since_db on each page of data from the CWL API (Fixes [#4](https://github.com/lukewaite/logstash-input-cloudwatch-logs/issues/4))

## v0.10.3  (2017-05-07)

### Fixed
* Fixed issue fetching log groups by prefix when there are more than 50 groups ([#22](https://github.com/lukewaite/logstash-input-cloudwatch-logs/pull/22))

## v0.10.2 (2017-04-20)

### Fixed
* Fixed bad merge on [#eb38dfd](https://github.com/lukewaite/logstash-input-cloudwatch-logs/commit/eb38dfdc072b4fd21e9c1d83ea306e2b6c5df37b) and restore compatibility with the Logstash 5.x events API ([#21](https://github.com/lukewaite/logstash-input-cloudwatch-logs/pull/21))

## v0.10.1 (2017-04-19)

### Fixed
* Fixed issue [#16](https://github.com/lukewaite/logstash-input-cloudwatch-logs/issues/16) which prevented loading the plugin ([#17](https://github.com/lukewaite/logstash-input-cloudwatch-logs/pull/17)) 

## v0.10.0 (2017-04-01)

### Added
* `log_group_prefix` parameter supporting ingesting a set of log groups matching a prefix ([#9](https://github.com/lukewaite/logstash-input-cloudwatch-logs/pull/9))

### Fixed
* Step back when throttled by Amazon ([#9](https://github.com/lukewaite/logstash-input-cloudwatch-logs/pull/9))

## v0.9.4 (2017-03-31)

### Fixed
* Fix autoloading of aws-sdk ([#15](https://github.com/lukewaite/logstash-input-cloudwatch-logs/pull/15))

## v0.9.3 (2016-12-22)

### Added
* Support for Logstash version 5.x ([#6e7cc5d](https://github.com/lukewaite/logstash-input-cloudwatch-logs/commit/6e7cc5decdcd7a8d8528d42a7b040b1d2f3a3490))

## v0.9.2 (2016-07-21)

### Added
* Initial publish to RubyGems

## v0.9.1 (2016-07-19)

### Added
* Support for Logstash version 2.x ([#8824ae9](https://github.com/lukewaite/logstash-input-cloudwatch-logs/commit/8824ae9899fa0e1d0a627796479824bc6f5c39b2))

## v0.9.0 (2015-07-06)

### Initial Release
* This is the initial release of the input
