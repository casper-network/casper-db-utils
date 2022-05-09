use std::result::Result;

use casper_types::Transfer;

use crate::db::{Database, DeserializationError};

pub struct TransferDatabase;

impl std::fmt::Display for TransferDatabase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "transfer")
    }
}

impl Database for TransferDatabase {
    fn db_name() -> &'static str {
        "transfer"
    }

    fn parse_element(bytes: &[u8]) -> Result<(), DeserializationError> {
        let _: Vec<Transfer> = bincode::deserialize(bytes)?;
        Ok(())
    }
}
