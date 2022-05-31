use std::{
    fmt::{Display, Formatter, Result as FormatterResult},
    result::Result,
};

use casper_types::bytesrepr::FromBytes;

use super::{Database, DeserializationError};

pub struct StateStoreDatabase;

impl Display for StateStoreDatabase {
    fn fmt(&self, f: &mut Formatter<'_>) -> FormatterResult {
        write!(f, "state_store")
    }
}

impl Database for StateStoreDatabase {
    fn db_name() -> &'static str {
        "state_store"
    }

    fn parse_element(bytes: &[u8]) -> Result<(), DeserializationError> {
        let _: u64 = FromBytes::from_bytes(bytes)?.0;
        Ok(())
    }
}
