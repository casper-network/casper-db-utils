use casper_node::types::DeployMetadata;
use std::{
    fmt::{Display, Formatter, Result as FormatterResult},
    result::Result,
};

use super::{Database, DeserializationError};

pub struct DeployMetadataDatabase;

impl Display for DeployMetadataDatabase {
    fn fmt(&self, f: &mut Formatter<'_>) -> FormatterResult {
        write!(f, "deploy_metadata")
    }
}

impl Database for DeployMetadataDatabase {
    fn db_name() -> &'static str {
        "deploy_metadata"
    }

    fn parse_element(bytes: &[u8]) -> Result<(), DeserializationError> {
        let _: DeployMetadata = bincode::deserialize(bytes)?;
        Ok(())
    }
}
