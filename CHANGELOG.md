# 0.14.1

## Fixed
- `python` codegen no longer uses `pydantic` type generation
- `Literal[...]` types can now be optional 

## Breaking Changes
- `python` codegen no longer uses `pydantic` type generation, migrating to `pydantic` should be an easy fix.
- `Literal[...]` types can now be optional, this can affect type-checkers.  

## Internal changes
- Update `sqlparser` to `0.60.0`

# 0.14.0

## Fixed
- `sqlalchemyv2` with `pydantic` type generation now also generates Naive & Aware datetimes for input types.
- `schema display` now accesses table info via information schema rather than generating a `select * from table` query.
- Quoted table names no longer break inference.

## Breaking Changes
- Decimal display no longer has any spaces

# 0.13.0

## Breaking Changes
- `time` no longer incorrectly generates a `timetz` internally.
- `char` and `varchar` types are no longer displayed as `char(???)` when length is unknown, instead the parentheses are omitted.
- `timestamp without timezone` and variants have been fixed to display `time zone` instead of `timezone`.
- `sql-infer` no longer errors out when an unknown type is encountered, it will instead generate an `unknown` type and code generation will handle it.

## Added
- `schema display` command will pretty print the tables' names, columns and datatypes.
- `schema lint` command will go through all user defined tables and point out potential problems.
    - Currently this only checks for `timestamp without time zone`, `time with time zone` and clashes between column and table names. `timestamp without time zone` may be necessary but is a product of forgetfulness most times. Use of `time with time zone` is actively discouraged by PostgreSQL docs and a column having the same name as the table causes ambiguity at best.  
- `sql-alchemy`, `sql-alchemy-async`, `sql-alchemy-v2` code generators now produce `Any` for unknown types.
- Support for enums have been added, `sql-alchemy` code generation options produce enums as `typing.Literal`s

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