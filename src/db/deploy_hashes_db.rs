use std::result::Result;

use casper_types::{bytesrepr::FromBytes, DeployHash};

use crate::db::{Database, DeserializationError};

pub struct DeployHashesDatabase;

impl std::fmt::Display for DeployHashesDatabase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "deploy_hashes")
    }
}

impl Database for DeployHashesDatabase {
    fn db_name() -> &'static str {
        "deploy_hashes"
    }

    fn parse_element(bytes: &[u8]) -> Result<(), DeserializationError> {
        let _: Vec<DeployHash> = FromBytes::from_bytes(bytes)
            .map_err(|_| DeserializationError::BytesreprError)?
            .0;
        Ok(())
    }
}
