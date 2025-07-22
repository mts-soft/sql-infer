# Unreleased

## BREAKING CHANGES
- Empty files will now generate empty queries

## Added
- Support for select items with aliases (e.g. `select x as y from z` )
- Partial support for ambiguous select items (e.g. `select x from y join z on <cond>` )

## Internal changes
- Major improvements to parsing logic to allow for more flexibility moving forward

# 0.9.0

## BREAKING CHANGES
- `sql-infer` has been split into `sql-infer-cli` and `sql-infer-core`, `sql-infer-cli` is the replacement binary for `sql-infer`.

- `init` command is no longer supported

### Previously Deprecated
- Database connection options can no longer be parsed from `sql-infer.toml`.


## Internal Changes

### Added
- Tokio runtime
### Removed
- Async std runtime