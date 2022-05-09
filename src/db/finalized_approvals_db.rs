use std::result::Result;

use casper_node::types::FinalizedApprovals;

use crate::db::{Database, DeserializationError};

pub struct FinalizedApprovalsDatabase;

impl std::fmt::Display for FinalizedApprovalsDatabase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "finalized_approvals")
    }
}

impl Database for FinalizedApprovalsDatabase {
    fn db_name() -> &'static str {
        "finalized_approvals"
    }

    fn parse_element(bytes: &[u8]) -> Result<(), DeserializationError> {
        let _: FinalizedApprovals = bincode::deserialize(bytes)?;
        Ok(())
    }
}
