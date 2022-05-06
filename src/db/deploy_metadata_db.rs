use casper_node::types::DeployMetadata;

use crate::db::{Database, Result};

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

    fn parse_element(bytes: &[u8]) -> Result<()> {
        let _: DeployMetadata = bincode::deserialize(bytes)?;
        Ok(())
    }
}
