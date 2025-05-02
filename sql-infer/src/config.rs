use std::{env, error::Error, fmt::Display};

use dotenvy::dotenv;
use serde::{Deserialize, Serialize};

const DATABASE_URL: &str = "DATABASE_URL";

#[derive(Debug, Clone)]
pub enum ConfigError {
    DbUrlNotFound,
}

impl Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigError::DbUrlNotFound => write!(
                f,
                "Database URL not found, please set the {DATABASE_URL} environment variable."
            ),
        }
    }
}

impl Error for ConfigError {}

#[derive(Serialize, Deserialize, Copy, Clone, Debug)]
#[serde(rename_all = "kebab-case")]
pub enum CodeGenOptions {
    Json,
    SqlAlchemy,
    SqlAlchemyAsync,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct DbInfo {
    host: String,
    port: u16,
    user: String,
    password: String,
    name: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
#[serde(rename_all = "kebab-case")]
pub struct ExperimentalFeatures {
    infer_nullability: Option<bool>,
    precise_output_datatypes: Option<bool>,
}

impl ExperimentalFeatures {
    fn into_feature_set(self) -> FeatureSet {
        FeatureSet {
            infer_nullability: self.infer_nullability.unwrap_or(false),
            precise_output_datatypes: self.precise_output_datatypes.unwrap_or(false),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "kebab-case")]
#[serde(untagged)]
pub enum QueryPath {
    Single(String),
    List(Box<[String]>),
}

#[derive(Clone, Debug)]
pub struct FeatureSet {
    pub infer_nullability: bool,
    pub precise_output_datatypes: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "kebab-case")]
#[must_use]
pub struct SqlInferOptions {
    pub path: QueryPath,
    pub target: Option<String>,
    pub mode: CodeGenOptions,
    pub database: Option<DbInfo>,
    pub experimental_features: ExperimentalFeatures,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "kebab-case")]
#[must_use]
pub struct SqlInferSubOptions {
    pub path: QueryPath,
    pub target: Option<String>,
    pub mode: Option<CodeGenOptions>,
    pub experimental_features: Option<ExperimentalFeatures>,
}

pub enum SqlInferOptionsEnum {
    Standard(SqlInferConfig),
    SubOptions(Vec<SqlInferSubOptions>),
}

impl SqlInferOptions {
    pub fn into_config(self) -> Result<SqlInferConfig, Box<dyn Error>> {
        dotenv()?;
        let mut db_url = None;
        if let Some(database) = self.database {
            tracing::warn!(
                "database in config is deprecated, use the DATABASE_URL environment variable."
            );
            db_url = Some(format!(
                "postgres://{}:{}@{}:{}/{}",
                database.user, database.password, database.host, database.port, database.name
            ));
        } else {
            for (key, value) in env::vars() {
                if key == DATABASE_URL {
                    db_url = Some(value.to_owned());
                }
            }
        };
        let Some(db_url) = db_url else {
            Err(ConfigError::DbUrlNotFound)?
        };
        Ok(SqlInferConfig {
            db_url,
            path: self.path,
            target: self.target,
            mode: self.mode,
            features: self.experimental_features.into_feature_set(),
        })
    }
}

pub struct SqlInferConfig {
    pub path: QueryPath,
    pub target: Option<String>,
    pub mode: CodeGenOptions,
    pub db_url: String,
    pub features: FeatureSet,
}

pub fn get_config() -> Result<SqlInferConfig, Box<dyn Error>> {
    let content = std::fs::read_to_string("sql-infer.toml")?;
    let options: SqlInferOptions = toml::from_str(&content)?;
    options.into_config()
}
