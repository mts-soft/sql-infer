# Unreleased

## Breaking Changes
- Empty queries cause errors

## Added
- `sql-infer-cli analyze` command  
    It is now possible to run `sql-infer-cli analyze [columns|tables|columns-with-db] <queries>` on a list of comma separated paths or sql queries in order to see what the parser is doing.
- Support for nested expressions (e.g. `(x)`) 
- Partial support for casts
- Support for values (e.g. `1`)
- Support for certain binary operations: `<`, `<=`, `>`, `>=`, `=`, `!=`, `and`, `or`, `xor`, `+`, `-`, `*`, `/`, `%` and `||`.
- Support for `count` by assuming it to be an integer value.

## Internal changes
- Information schema is now computed and cached for every `Column`.  

# 0.10.0

## Added
- Support for select items with aliases (e.g. `select x as y from z` )
- Support for ambiguous select items (e.g. `select x from y join z on <cond>` )

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