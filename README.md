# sql-py
Generates DBAPI2 code, uses SQLAlchemy Core.

## Check query validity 
```./sql-py check-query --db <db-connection-url> --sql <query.sql>```
- This will verify the query against the database without running it and show expected parameter types and the return type.


```./sql-py create-query --db-url <db-connection-url> --sql-dir <directory>```
- This will create a complete python file that uses sqlalchemy. Each file in the directory should be named as: `<func_name>.sql`. 
- The program will create a function for each query called <func_name> with the appropriate signature and return types.  


## Options
### `--debug` 
- Prints debug information
### `--experimental-parser`
- Parses the given query to find which table and column each item originates from. Currently supports a small subset of SQL however should be usable for most shorter queries. 
- This feature is currently only used to infer nullability but will be expanded to also help infer types more accurately. 