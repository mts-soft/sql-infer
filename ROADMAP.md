In no particular order:

- [ ] Migrate from async-std to tokio
- [ ] Separate sql-infer CLI implementation from sql-infer functionality
    - This will result in `sql-infer-core` that strictly is a type inference library and an `sql-infer-cli` crate that will include the CLI part of the functionality. 
- [ ] Setup regression tests for `sql-infer-core`
    - This will require an active postgreSQL instance on the host machine   
- [ ] De-couple code generation as much as possible from `sql-infer-core`
- [ ] Support table aliases (e.g. `select u.id from users u` will currently break nullability inference)
- [ ] Better determine scope
    - sql-infer can theoretically support some amount of templating in a sound manner. This could be as simple as parametrizing `asc`/`desc` for `order by` or could go as far as parametrizing column names and exhausting all possibilities to ensure safety when there are multiple.
- [ ] No panic
- [ ] Descriptive error messages
- [ ] Remove serde dependency on sql-infer-core