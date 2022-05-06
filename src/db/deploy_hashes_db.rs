use casper_types::{bytesrepr::FromBytes, DeployHash};

use crate::db::{Database, Error, Result};

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

    fn parse_element(bytes: &[u8]) -> Result<()> {
        let _: Vec<DeployHash> = FromBytes::from_bytes(bytes)
            .map_err(|_| Error::BytesreprError)?
            .0;
        Ok(())
    }
}
