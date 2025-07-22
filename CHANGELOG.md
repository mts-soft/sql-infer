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