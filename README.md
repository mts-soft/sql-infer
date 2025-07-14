# sql-infer

Compute input/output types for SQL queries given a database to connect to.

Limitations:
- Only supports python >= 3.12
- Supports postgreSQL only, technically MariaDB support is trivially possible but there is no motivation to implement it.
- A single configuration file will only support a single output file making codebases potentially harder to organize.
- There is no support for bulk operations.
- This is a project that has been put together very quickly, there is no proper SQL parsing or input validation. Determining parameters are done via regex. 

## Example Config file

```toml
path = "<path/to/input/directory>" or ["<path1>", "<path2>"]
target = "<path/to/output/file>"
mode = "json" # "sql-alchemy" and "sql-alchemy-async" are alternative options

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

- All

### `"sql-alchemy-async"`

Generate type-safe async SQL Alchemy Core code using the provided typing information.

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


## Project Structure 

- Below is the recommended project structure, it is also possible to have sub directories within queries and add them to the searched `path` in `sql-infer.toml`.
```
project
|   src
|   |  ...
|   |  queries.py
|   queries
|   | somequery.sql
|   | someotherquery.sql
|   sql-infer.toml
```

- Upon running `sql-infer generate` sql-infer will look for `sql-infer.toml` within the current directory, look for the `DATABASE_URL` environment variable and attempt to connect to the database. Keep in mind that there is very minimal sanitization being done in `sql-infer` and it should strictly be used with trusted input and in a trusted environment.

## Why was sql-infer made?
We were originally using SQL Alchemy ORM within the organization as minimal research suggests it to be the go-to. We had a couple problems with it:
- We were really just thinking of SQL queries and then thinking of how we would write it in SQL Alchemy ORM.
- We faced type checking problems with joins where the types would not be marked as nullable and we'd have to simultanouesly fight both SQL Alchemy, Pyright and Ruff.
- We were not using any of the features provided by the ORM to a degree that would justify keeping it.

## How does it work?
- SQL Infer uses `sqlx` as this was initially the easiest way to get parameter & column types for a given query however this is actually relatively simple to accomplish without `sqlx` as well. `sqlx` remains in the repository thus `sql-infer` can infer all types that default `sqlx` can without any extra work.
- Due to the limitations with python type checking, sql-infer generates code to be output into an existing file.
- The experimental `infer-nullability` and `precise-output-data-types` are already used within internal codebases. THey are called experimental because it is possible to have them produce incorrect results by confusing the very naive system that uses string equality to determine the source table for each column.


## Example sql-infer usage
- It is recommended to have sql-infer output be formatted if `sql-alchemy` or `sql-alchemy-async` is being used. If the `json` output format is being used, you are probably already making your own code generation on top if it.

Imagine a file named `example.sql` with the below content
```sql
select name from users where id = :id;
```

given a table named `users` that has an `id` column of type: serial2, serial4, serial8, int2, int4 or int8 and a `name` of type `char`, `varchar`, `text` of any length, this query will produce:

```python
@dataclass
class ExampleOutput:
    name: str


async def example(conn: AsyncConnection, id: int | None) -> DbOutput[ExampleOutput]:
    result = await conn.execute(
        text("""select name from users where id = :id;"""), {"id": id}
    )
    return DbOutput(ExampleOutput(*row) for row in result)  # type: ignore
```

`DbOutput` is defined once per file as follows

```python
@dataclass
class DbOutput[T]:
    inner: Generator[T]

    def first(self) -> T | None:
        """Get the first result if available."""
        try:
            return self.inner.__next__()
        except BaseException:
            return None

    def all(self) -> Generator[T]:
        return self.inner
```