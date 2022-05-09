use std::result::Result;

use casper_node::types::DeployMetadata;

use crate::db::{Database, DeserializationError};

pub struct DeployMetadataDatabase;

impl std::fmt::Display for DeployMetadataDatabase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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
