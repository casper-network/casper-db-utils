use std::{
    fmt::{Display, Formatter, Result as FormatterResult},
    result::Result,
};

use casper_types::{bytesrepr::FromBytes, DeployHash};

use super::{Database, DeserializationError};

pub struct TransferHashesDatabase;

impl Display for TransferHashesDatabase {
    fn fmt(&self, f: &mut Formatter<'_>) -> FormatterResult {
        write!(f, "transfer_hashes")
    }
}

impl Database for TransferHashesDatabase {
    fn db_name() -> &'static str {
        "transfer_hashes"
    }

    fn parse_element(bytes: &[u8]) -> Result<(), DeserializationError> {
        let _: Vec<DeployHash> = FromBytes::from_bytes(bytes)?.0;
        Ok(())
    }
}
