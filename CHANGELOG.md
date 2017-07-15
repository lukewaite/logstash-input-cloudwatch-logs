# Release Notes for v1.x

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [v1.0.0] (2017-07-15)

### Added
* Implement `start_position` setting ([#28](https://github.com/lukewaite/logstash-input-cloudwatch-logs/issues/28))
* Allow log_group to be an array of groups (or prefixes if enabled) ([#13](https://github.com/lukewaite/logstash-input-cloudwatch-logs/issues/13))

### Fixed
* Ensure the plugin stops properly
* Relax the contstraint on `logstash-mixin-aws` supporting Logstash 2.4
* Refactored ingestion, fixes multiple memory leaks (Fixes [#24](https://github.com/lukewaite/logstash-input-cloudwatch-logs/issues/4))
* Pull only log_events since last ingestion (Fixes [#10](https://github.com/lukewaite/logstash-input-cloudwatch-logs/issues/10))
* Incrementally write to since_db on each page of data from the CWL API (Fixes [#4](https://github.com/lukewaite/logstash-input-cloudwatch-logs/issues/4))

[Unreleased]: https://github.com/lukewaite/logstash-input-cloudwatch-logs/compare/v1.0.0...HEAD
[v1.0.0]: https://github.com/lukewaite/logstash-input-cloudwatch-logs/compare/v0.10.3...v1.0.0
