# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Comprehensive GitHub Pages documentation
- MkDocs-based documentation site
- API reference documentation

## [0.2.5] - 2024-XX-XX

### Added
- Enhanced dimension conflict resolution with automatic renaming
- Improved nested structure support for complex BigQuery types
- Better error handling with continue-on-error mode
- Advanced measure types support (percentile, approximate count_distinct)

### Changed
- Refactored dimension generation for better maintainability
- Improved SQL syntax generation for nested views
- Enhanced catalog-driven type analysis

### Fixed
- Fixed nested SQL syntax generation for complex LookML views
- Resolved dimension naming conflicts in nested structures
- Fixed CLI filtering integration test failures

## [0.2.4] - 2024-XX-XX

### Added
- Support for Swedish characters (ä, ö, å) in column names
- Enhanced quote_column_name_if_needed function for non-ASCII characters

### Fixed
- Column names with non-ASCII characters now properly quoted in LookML

## [0.2.3] - 2024-XX-XX

### Added
- Comprehensive unit test coverage for dimension and view generators
- Performance testing capabilities with pytest-benchmark
- Enhanced error handling and logging

### Changed
- Improved test organization and structure
- Better separation of concerns in generator classes

## [0.2.2] - 2024-XX-XX

### Added
- Support for complex nested BigQuery structures
- Hierarchy-based nested array dimension detection
- Generic LKML structure comparison for end-to-end tests

### Fixed
- Nested array dimension handling in complex structures
- STRUCT field exclusion logic for parent fields with children

## [0.2.1] - 2024-XX-XX

### Added
- ISO week and year fields support
- Custom timeframes configuration
- Locale file generation support
- Advanced model filtering options

### Changed
- Improved type mapping from BigQuery to Looker
- Enhanced nested structure handling

## [0.2.0] - 2024-XX-XX

### Added
- Support for dbt exposures
- Advanced model filtering (include/exclude lists)
- YAML configuration file support
- Continue-on-error mode
- Enhanced measure types and precision settings

### Changed
- Major refactoring of core generation logic
- Improved performance for large projects
- Better error messages and logging

### Fixed
- Various bugs in nested structure handling
- Type mapping edge cases

## [0.1.0] - 2024-XX-XX

### Added
- Initial release
- Basic LookML view generation from dbt models
- Support for BigQuery data types
- Automatic join detection
- Custom measure definition support
- dbt tags support

### Features
- Fast generation (2800+ views in ~6 seconds)
- Automatic type mapping
- Nested structure support
- Command-line interface
