use super::versioned_database::VersionedDatabases;
#[cfg(test)]
use crate::common::db::MockData;
use crate::common::structs::{ApprovalsHashes, Transfers};
use casper_types::{
    Approval, BlockBody, BlockHash, BlockHeader, BlockSignatures, Digest, Transaction,
    TransactionHash, execution::ExecutionResult,
};
use std::collections::BTreeSet;

pub(crate) fn block_body_database() -> VersionedDatabases<Digest, BlockBody> {
    VersionedDatabases::<Digest, BlockBody>::new("block_body", "block_body_v2")
}
pub(crate) fn block_header_database() -> VersionedDatabases<BlockHash, BlockHeader> {
    VersionedDatabases::<BlockHash, BlockHeader>::new("block_header", "block_header_v2")
}
pub(crate) fn block_metadata_database() -> VersionedDatabases<BlockHash, BlockSignatures> {
    VersionedDatabases::<BlockHash, BlockSignatures>::new("block_metadata", "block_metadata_v2")
}
pub(crate) fn transactions_database() -> VersionedDatabases<TransactionHash, Transaction> {
    VersionedDatabases::<TransactionHash, Transaction>::new("deploys", "transactions")
}
pub(crate) fn execution_results_database() -> VersionedDatabases<TransactionHash, ExecutionResult> {
    VersionedDatabases::<TransactionHash, ExecutionResult>::new(
        "deploy_metadata",
        "execution_results",
    )
}
pub(crate) fn versioned_transfers_database() -> VersionedDatabases<BlockHash, Transfers> {
    VersionedDatabases::<BlockHash, Transfers>::new("transfer", "versioned_transfers")
}
pub(crate) fn versioned_finalized_approvals_database()
-> VersionedDatabases<TransactionHash, BTreeSet<Approval>> {
    VersionedDatabases::<TransactionHash, BTreeSet<Approval>>::new(
        "finalized_approvals",
        "versioned_finalized_approvals",
    )
}
pub(crate) fn approvals_hashes_database() -> VersionedDatabases<BlockHash, ApprovalsHashes> {
    VersionedDatabases::<BlockHash, ApprovalsHashes>::new(
        "approvals_hashes",
        "versioned_approvals_hashes",
    )
}

#[cfg(test)]
pub(crate) fn mock_database() -> VersionedDatabases<TransactionHash, MockData> {
    VersionedDatabases::<TransactionHash, MockData>::new("legacy_mock", "mock")
}
