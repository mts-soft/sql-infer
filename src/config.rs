use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Copy, Clone, Debug)]
#[serde(rename_all = "kebab-case")]
pub enum CodeGenOptions {
    Json,
    SqlAlchemy,
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

#[derive(Clone, Debug)]
pub struct FeatureSet {
    pub infer_nullability: bool,
    pub precise_output_datatypes: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "kebab-case")]
#[must_use]
pub struct SqlInferOptions {
    pub path: String,
    pub target: Option<String>,
    pub mode: CodeGenOptions,
    pub database: DbInfo,
    pub experimental_features: ExperimentalFeatures,
}

impl SqlInferOptions {
    pub fn into_config(self) -> SqlInferConfig {
        let db_url = format!(
            "postgres://{}:{}@{}:{}/{}",
            self.database.user,
            self.database.password,
            self.database.host,
            self.database.port,
            self.database.name
        );
        SqlInferConfig {
            db_url,
            path: self.path,
            target: self.target,
            mode: self.mode,
            features: self.experimental_features.into_feature_set(),
        }
    }
}

pub struct SqlInferConfig {
    pub path: String,
    pub target: Option<String>,
    pub mode: CodeGenOptions,
    pub db_url: String,
    pub features: FeatureSet,
}
