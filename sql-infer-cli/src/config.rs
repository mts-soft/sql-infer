use std::{env, error::Error, fmt::Display, path::PathBuf};

use dotenvy::dotenv;
use serde::{Deserialize, Serialize};

use crate::codegen::sqlalchemy_v2::{ArgumentMode, TypeGen};

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

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub struct Features {
    infer_nullability: Option<bool>,
    precise_output_datatypes: Option<bool>,
}

impl Features {
    pub fn nullability(&self) -> bool {
        self.infer_nullability.unwrap_or(false)
    }

    pub fn text_length(&self) -> bool {
        self.precise_output_datatypes.unwrap_or(false)
    }

    pub fn decimal_precision(&self) -> bool {
        self.precise_output_datatypes.unwrap_or(false)
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CodeGenerator {
    Json,
    #[serde(rename_all = "kebab-case")]
    SqlAlchemyV2 {
        #[serde(default = "bool::default")]
        r#async: bool,
        #[serde(default = "ArgumentMode::default")]
        argument_mode: ArgumentMode,
        #[serde(default = "TypeGen::default")]
        type_gen: TypeGen,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum CodeGenSource {
    Single(PathBuf),
    List(Vec<PathBuf>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[must_use]
pub struct TomlConfig {
    path: CodeGenSource,
    target: PathBuf,
    mode: CodeGenerator,
    #[serde(default = "Default::default")]
    experimental_features: Features,
}

#[derive(Debug, Clone)]
pub struct SqlInferConfig {
    pub source: Vec<PathBuf>,
    pub target: PathBuf,
    pub mode: CodeGenerator,
    pub experimental_features: Features,
}

pub fn db_url() -> Result<String, Box<dyn Error>> {
    dotenv()?;
    let mut db_url = None;
    for (key, value) in env::vars() {
        if key == DATABASE_URL {
            db_url = Some(value.to_owned());
        }
    }

    Ok(db_url.ok_or(ConfigError::DbUrlNotFound)?)
}

impl SqlInferConfig {
    pub fn from_toml_config(config: TomlConfig) -> Result<Self, Box<dyn Error>> {
        let source = match config.path {
            CodeGenSource::Single(item) => vec![item],
            CodeGenSource::List(items) => items,
        };

        Ok(Self {
            source,
            target: config.target,
            mode: config.mode,
            experimental_features: config.experimental_features,
        })
    }
}
