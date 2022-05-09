use std::result::Result;

use casper_node::types::Deploy;

use crate::db::{Database, DeserializationError};

pub struct DeployDatabase;

impl std::fmt::Display for DeployDatabase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "deploys")
    }
}

impl Database for DeployDatabase {
    fn db_name() -> &'static str {
        "deploys"
    }

    fn parse_element(bytes: &[u8]) -> Result<(), DeserializationError> {
        let _: Deploy = bincode::deserialize(bytes)?;
        Ok(())
    }
}
