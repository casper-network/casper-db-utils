use casper_node::types::FinalizedApprovals;

use crate::db::{Database, Result};

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

    fn parse_element(bytes: &[u8]) -> Result<()> {
        let _: FinalizedApprovals = bincode::deserialize(bytes)?;
        Ok(())
    }
}
