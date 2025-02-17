# sql-infer

Generates DBAPI2 code, uses SQLAlchemy Core.

## Init config file

`./sql-infer init`

## Check query validity and generate code

`./sql-infer generate`

## Debug

Each command can be run with `--debug` to print more information

## Example Config file

```toml
path = "<path/to/input/directory>"
target = "<path/to/output/file>"
mode = "json" # or "sql-alchemy"

[database]
host = "127.0.0.1"
port = 5432
user = "root"
password = ""
name = ""

[experimental-features]
infer-nullability = true
precise-output-datatypes = true
```

## Modes

### `"json"`

Serialize typing information to a JSON file. JSON is always the primarily supported code generation option and will support every feature.

Supported features:

- All

### `"sql-alchemy"`

Generate type-safe SQL Alchemy Core code using the provided typing information.

Supported features:

- Infer nullability

## Experimental Features

These features may be removed at any time

### Infer Nullability

Infer whether the output type is nullable or not to the extent possible.

This currently works for queries that only use inner/left/right/cross joins and queries that return a column as is without any modification.

### Precise Output Datatypes

Infer additional information relating to the datatype.

- with/without timezone for Timestamp and Time
- Char and VarChar lengths
- Decimal precision and precision radix

This currently works for queries that only use inner/left/right/cross joins and queries that return a column as is without any modification.
