# BigGraphite
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/)
and this project adheres to [Semantic Versioning](http://semver.org/).

## [Unreleased]

## [0.8.8] - 2017-07-11
### Fixed
- Bug in cleanup code (would remove intermediate directories)

## [0.8.7] - 2017-07-07
### Fixed
- Infinite caching for metadata.

## [0.8.6] - 2017-06-15
### Fixed
- Infinite caching for metadata.

## [0.8.5] - 2017-06-15
### Fixed
- Fixed the time info of empty timeseries
- Fix error handling in repair()/clean()

### New
- Added bgutil stats
- Better caching for find()
- Use new Graphite-Web API for FindQuery
- Make read_on/updated_on more configurable

## [0.8.4] - 2017-05-15
### Fixed
- Better logging

### New
- Add `bgutil copy`
- Better exception in carbon.py when metric can't be found

## [0.8.3] - 2017-04-25
- Nothing, just an issue with pypi uploads.

## [0.8.2] - 2017-04-25
### Fixed
- Fix ipython and cassandra versions

## [0.8.1] - 2017-04-05
### Fixed
- Better handling of Cassandra exceptions
- Better handling of carbonlink
- Fix metric reporting interval

## [0.8.0] - 2017-03-10
### Added
- Configurable consistency settings (#242)
- [BREAKING] New column (read_on) in metric_metadata for adding statistics on read metrics (#107)
  need to run the following cql to migrate theschema
```SQL
   ALTER TABLE biggraphite_metadata.metrics_metadata
   ADD read_on timeuuid;
```
- Added a tool to replay graphite traffic
- Support Carbon Link protocol

### Fixed
- Fix multi-cluster support
- Be more robust when metadata is missing

### Changed
- Added some randomness to cache expiration

## [0.7.0]
### Added
- Prometheus support and additional metrics
- Add support for multiple replicas/writers
- Microbenchmarks
- Asynchronous creation of metrics in carbon
- Better bg-import-whisper / bg-generate-sstables + helper scripts

### Fixed
- Multiple bugs with the disk cache
- Multiple clean/repair bugs

### Changed
- [Breaking] Cassandra schema as been changed to separate stage0 and aggregated
  metrics. There is no upgrade procedure as this is a pre-release.
- [Breaking] `bgutil syncdb` needs to be run during initial install

## [0.6.0] - 2016-11-25
### Added
- Initial release with this CHANGELOG file

### Changed
- We are going to do releases from now on


[Unreleased]: https://github.com/criteo/biggraphite/compare/v0.8.8...HEAD
[0.8.7]: https://github.com/criteo/biggraphite/compare/v0.8.7...v0.8.8
[0.8.6]: https://github.com/criteo/biggraphite/compare/v0.8.6...v0.8.7
[0.8.5]: https://github.com/criteo/biggraphite/compare/v0.8.5...v0.8.6
[0.8.5]: https://github.com/criteo/biggraphite/compare/v0.8.4...v0.8.5
[0.8.4]: https://github.com/criteo/biggraphite/compare/v0.8.3...v0.8.4
[0.8.3]: https://github.com/criteo/biggraphite/compare/v0.8.2...v0.8.3
[0.8.2]: https://github.com/criteo/biggraphite/compare/v0.8.1...v0.8.2
[0.8.1]: https://github.com/criteo/biggraphite/compare/v0.8.0...v0.8.1
[0.8.0]: https://github.com/criteo/biggraphite/compare/v0.7.0...v0.8.0
[0.7.0]: https://github.com/criteo/biggraphite/compare/v0.6.0...v0.7.0
[0.6.0]: https://github.com/criteo/biggraphite/compare/v0.6.0
