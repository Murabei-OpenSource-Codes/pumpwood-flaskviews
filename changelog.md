# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.3.33] - 2025-11-07
### Added
- Add env variable `PUMPWOOD_FLASKVIEWS__SERIALIZER_FK_CACHE_TIMEOUT` to adjust
  the timeout for cache to reduce calls at other microservice to bring
  foreign key objects on retrieve and list end-points.

### Fix
- No fix.

### Changed
- No changes.

## [1.3.28] - 2025-10-31
### Added
- No add cache to informational end-point.
- Create a `config.py` file to centralize configurations using environment
  variables.

### Fix
- No fix.

### Changed
- No changes.

## [1.3.24] - 2025-10-31
### Added
- No adds.

### Fix
- Pivot column default was using order_by information.

### Changed
- No changes.

## [1.3.13] - 2025-09-19

### Added
- Add audit fields for object creation and update.

### Changed
- Change behavior of fill_options, getting information from serializer field
  before SQLAlchemy. Consistent with value attribution and validations.

### Removed
- No removes.

## [1.3.10] - 2025-09-19

### Added
- No adds.

### Changed
- Fix fill options for primary non-composite.

### Removed
- No removes.

## [1.3.9] - 2025-09-19

### Added
- Add choice field choices validation at serializer.

### Changed
- No changes.

### Removed
- No removes.

## [1.3.5] - 2025-09-09

### Added
- Cache token autorization.

### Changed
- No changes.

### Removed
- No removes.

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
