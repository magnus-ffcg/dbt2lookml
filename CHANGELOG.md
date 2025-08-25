# Changelog

Recent and upcoming changes to dbt2lookml

## v0.1.0

- Initial release - bunch of stuff added. Heavily inspired by dbt2looker and dbt2lookml - functionality wise its the same but heavily reworked, just refactored and added new features (better for support for nested views and more)

## v0.1.0 - v0.2.2

- General stability to the code

## v0.2.3

- Introduced a safe_name method for dimension names in case of column name does not match looker rules.

## v0.2.4

- Introduced a conflict check for dimension groups that might create conflicts with regular dimensions names. Those timeframes will then be commented out.

## v0.2.5

- Introduced a config file.
- Added possibility to override timeseries.
