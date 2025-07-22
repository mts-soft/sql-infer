use regex::Regex;
use std::error::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParametrizedQuery {
    pub raw_query: String,
    pub params: Vec<String>,
}

fn split_query(mut query: &str) -> Vec<&str> {
    let mut split_query = vec![];
    if query.starts_with('\'') {
        split_query.push(&query[..1]);
        query = &query[1..];
    }
    let mut in_quotes = false;
    let mut in_double_quotes = false;
    let mut last = 0;
    for (idx, char) in query.char_indices() {
        // TODO: clean up duplicate
        // SQL Quotes are escaped by doubling up so we do not check for backslashes.
        if char == '\'' {
            in_quotes = !in_quotes;
            let end = match in_quotes {
                true => idx,
                false => idx + 1,
            };
            split_query.push(&query[last..end]);
            last = end;
        }
        // SQL Double quotes are escaped by doubling up so we do not check for backslashes.
        if char == '\"' {
            in_double_quotes = !in_double_quotes;
            let end = match in_double_quotes {
                true => idx,
                false => idx + 1,
            };
            split_query.push(&query[last..end]);
            last = end;
        }
    }
    split_query.push(&query[last..]);
    split_query
}

pub fn parse_into_postgres(query: &str) -> Result<ParametrizedQuery, Box<dyn Error>> {
    /*
    TODO: Using regex really is not the proper way to parse SQL query identifiers, write a proper tokenizer or use sqlparse.
     */
    let mut params = Vec::new();
    let placeholder_pattern = Regex::new(r":([a-z]|[A-Z]|_)([a-z]|[A-Z]|_|[0-9])*")?;
    let split_query = split_query(query);

    let mut postgres_query = String::new();
    for (id, query) in split_query.into_iter().enumerate() {
        if id % 2 == 1 {
            postgres_query += query;
            continue;
        }
        let mut head = 0;
        for matches in placeholder_pattern.captures_iter(query) {
            let placeholder = matches.get(0).unwrap();
            let start = placeholder.start();
            if query
                .get(..start)
                .is_some_and(|slice| slice.trim().ends_with(":"))
            {
                // Two colons is indicative of casting
                // We do not handle this inside of the regex as the match would include the character prior
                continue;
            }
            postgres_query += &query[head..start];
            let param_name = &placeholder.as_str()[1..];
            let param_index = 1 + params
                .iter()
                .position(|param| param == param_name)
                .unwrap_or_else(|| {
                    params.push(param_name.to_string());
                    params.len() - 1
                });
            postgres_query += &format!("${param_index}");
            head = start + placeholder.len();
        }
        postgres_query += &query[head..];
    }
    Ok(ParametrizedQuery {
        raw_query: postgres_query,
        params,
    })
}
