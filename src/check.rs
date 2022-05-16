use std::result::Result;

use crate::db::{
    db_env, BlockBodyDatabase, BlockBodyMerkleDatabase, BlockHeaderDatabase, BlockMetadataDatabase,
    Database, DeployDatabase, DeployHashesDatabase, DeployMetadataDatabase, Error,
    FinalizedApprovalsDatabase, ProposerDatabase, StateStoreDatabase, TransferDatabase,
    TransferHashesDatabase,
};

pub fn check_db(
    path: &str,
    failfast: bool,
    specific: Option<&str>,
    start_at: usize,
) -> Result<(), Error> {
    let env = db_env(path.into()).expect("Failed to initialize DB environment");
    if let Some(db_name) = specific {
        match db_name.trim() {
            "block_body" => BlockBodyDatabase::check_db(&env, failfast, start_at),
            "block_body_merkle" => BlockBodyMerkleDatabase::check_db(&env, failfast, start_at),
            "block_header" => BlockHeaderDatabase::check_db(&env, failfast, start_at),
            "block_metadata" => BlockMetadataDatabase::check_db(&env, failfast, start_at),
            "deploy_hashes" => DeployHashesDatabase::check_db(&env, failfast, start_at),
            "deploy_metadata" => DeployMetadataDatabase::check_db(&env, failfast, start_at),
            "deploys" => DeployDatabase::check_db(&env, failfast, start_at),
            "finalized_approvals" => FinalizedApprovalsDatabase::check_db(&env, failfast, start_at),
            "proposers" => ProposerDatabase::check_db(&env, failfast, start_at),
            "state_store" => StateStoreDatabase::check_db(&env, failfast, start_at),
            "transfer" => TransferDatabase::check_db(&env, failfast, start_at),
            "transfer_hashes" => TransferHashesDatabase::check_db(&env, failfast, start_at),
            _ => panic!("Database {} not found.", db_name),
        }
    } else {
        // Sanity check for `start_at`, already validated in arg parser.
        assert_eq!(start_at, 0);
        BlockBodyDatabase::check_db(&env, failfast, start_at)?;
        BlockBodyMerkleDatabase::check_db(&env, failfast, start_at)?;
        BlockHeaderDatabase::check_db(&env, failfast, start_at)?;
        BlockMetadataDatabase::check_db(&env, failfast, start_at)?;
        DeployHashesDatabase::check_db(&env, failfast, start_at)?;
        DeployMetadataDatabase::check_db(&env, failfast, start_at)?;
        DeployDatabase::check_db(&env, failfast, start_at)?;
        FinalizedApprovalsDatabase::check_db(&env, failfast, start_at)?;
        ProposerDatabase::check_db(&env, failfast, start_at)?;
        StateStoreDatabase::check_db(&env, failfast, start_at)?;
        TransferDatabase::check_db(&env, failfast, start_at)?;
        TransferHashesDatabase::check_db(&env, failfast, start_at)
    }
}
