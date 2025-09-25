# Unreleased

## Breaking Changes
- `char` and `varchar` types are no longer displayed as `char(???)` when length is unknown, instead the parentheses are omitted.
- `timestamp without timezone` and variants have been fixed to display `time zone` instead of `timezone`.
- `sql-infer` no longer errors out when an unknown type is encountered, it will instead generate an `unknown` type and code generation will handle it.   

## Added
- `schema display` command will pretty print the tables' names, columns and datatypes.
    - Table names are accessed through the DB but a `select * from {table}` is generated for each table with no care for escaping, this will be fixed before a release.
- `schema lint` command will go through all user defined tables and point out potential problems.
    - Currently this only checks for `timestamp without time zone` and `time with time zone` as the former is to be avoided in most cases and the latter should almost never be necessary.
- `sql-alchemy`, `sql-alchemy-async`, `sql-alchemy-v2` code generators now produce `Any` for unknown types.

# 0.12.0

## Added
- Better error messages when the program fails to find `DATABASE_URL` or `sql-infer.toml`.
- `sql-alchemy-v2` code generation to allow for more customization.  
    ```toml
    [mode.sql-alchemy-v2]
    async = false # or true
    type-gen = "python" # or pydantic
    argument-mode = "positional" # or keyword
    ```
- `async = "true"` generates async code.
- `type-gen = "python"` allows for NaiveDatetime and AwareDatetime types to be generated.
- `argument-mode = "keyword"` forces every argument except for the connection object to be specified by a keyword. This is done to minimize errors due to argument order depending on SQL queries.     

# 0.11.0

## Breaking Changes
- Empty queries cause errors

## Added
- `sql-infer-cli analyze` command  
    It is now possible to run `sql-infer-cli analyze [columns|tables|columns-with-db] <queries>` on a list of comma separated paths or sql queries in order to see what the parser is doing.
- Support for nested expressions (e.g. `(x)`) 
- Partial support for casts
- Support for values (e.g. `1`, `value`)
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