# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.3.4] - 2025-09-09

### Added
- Disk cache for get requests.
- User of orjson for object dump and load.

### Changed
- No changes.

### Removed
- No removes.

## [1.3.3] - 2025-03-21

### Added
- Treatment of unique errors on Postgres/SQLAlchemy.

### Changed
- Change the behavior of related fields to respect the default fields, even
  if foreign_key or related.

### Removed
- No removes.

## [1.2.1] - 2025-03-21

### Added
- Cache data from MicroserviceRelatedField at request reducing request time
  mainlly for list end-points.

### Changed
- No changes.

### Removed
- No removes.


## [1.2.0] - 2025-03-21

### Added
- Added possibility to use composite primary keys to fetch data for fields
  `MicroserviceForeignKeyField` and `MicroserviceRelatedField`

### Changed
- No changes.

### Removed
- No removes.
