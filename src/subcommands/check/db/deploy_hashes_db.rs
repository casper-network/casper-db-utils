use std::{
    fmt::{Display, Formatter, Result as FormatterResult},
    result::Result,
};

use casper_types::{bytesrepr::FromBytes, DeployHash};

use super::{Database, DeserializationError};

pub struct DeployHashesDatabase;

impl Display for DeployHashesDatabase {
    fn fmt(&self, f: &mut Formatter<'_>) -> FormatterResult {
        write!(f, "deploy_hashes")
    }
}

impl Database for DeployHashesDatabase {
    fn db_name() -> &'static str {
        "deploy_hashes"
    }

    fn parse_element(bytes: &[u8]) -> Result<(), DeserializationError> {
        let _: Vec<DeployHash> = FromBytes::from_bytes(bytes)?.0;
        Ok(())
    }
}
