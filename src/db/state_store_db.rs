use std::result::Result;

use casper_types::bytesrepr::FromBytes;

use crate::db::{Database, DeserializationError};

pub struct StateStoreDatabase;

impl std::fmt::Display for StateStoreDatabase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "state_store")
    }
}

impl Database for StateStoreDatabase {
    fn db_name() -> &'static str {
        "state_store"
    }

    fn parse_element(bytes: &[u8]) -> Result<(), DeserializationError> {
        let _: u64 = FromBytes::from_bytes(bytes)
            .map_err(|_| DeserializationError::BytesreprError)?
            .0;
        Ok(())
    }
}
