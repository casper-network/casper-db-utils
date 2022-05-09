use std::result::Result;

use casper_types::{bytesrepr::FromBytes, DeployHash};

use crate::db::{Database, DeserializationError};

pub struct TransferHashesDatabase;

impl std::fmt::Display for TransferHashesDatabase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "transfer_hashes")
    }
}

impl Database for TransferHashesDatabase {
    fn db_name() -> &'static str {
        "transfer_hashes"
    }

    fn parse_element(bytes: &[u8]) -> Result<(), DeserializationError> {
        let _: Vec<DeployHash> = FromBytes::from_bytes(bytes)
            .map_err(|_| DeserializationError::BytesreprError)?
            .0;
        Ok(())
    }
}
