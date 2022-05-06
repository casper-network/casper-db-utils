mod block_body_db;
mod block_body_merkle_db;
mod block_header_db;
mod block_metadata_db;
mod deploy_hashes_db;
mod deploy_metadata_db;
mod deploys_db;
mod finalized_approvals_db;
mod proposers_db;
mod state_store_db;
mod transfer_db;
mod transfer_hashes_db;

pub use block_body_db::BlockBodyDatabase;
pub use block_body_merkle_db::BlockBodyMerkleDatabase;
pub use block_header_db::BlockHeaderDatabase;
pub use block_metadata_db::BlockMetadataDatabase;
pub use deploy_hashes_db::DeployHashesDatabase;
pub use deploy_metadata_db::DeployMetadataDatabase;
pub use deploys_db::DeployDatabase;
pub use finalized_approvals_db::FinalizedApprovalsDatabase;
pub use proposers_db::ProposerDatabase;
pub use state_store_db::StateStoreDatabase;
pub use transfer_db::TransferDatabase;
pub use transfer_hashes_db::TransferHashesDatabase;

use std::path::PathBuf;

use lmdb::{Cursor, Environment, EnvironmentFlags, RoCursor, Transaction};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("failed database operation")]
    LmbdError(#[from] lmdb::Error),
    #[error("failed parsing struct with bincode")]
    BincodeError(#[from] bincode::Error),
    #[error("failed parsing struct with bytesrepr")]
    BytesreprError,
}

pub(crate) type Result<T> = std::result::Result<T, Error>;

pub fn db_env(path: PathBuf) -> Result<Environment> {
    let env = Environment::new()
        .set_flags(
            EnvironmentFlags::NO_SUB_DIR
                | EnvironmentFlags::NO_TLS
                | EnvironmentFlags::NO_READAHEAD,
        )
        .set_max_dbs(12)
        .open(&path)?;
    Ok(env)
}

#[allow(unused)]
pub enum Databases {
    BlockHeader,
    BlockBodyV1,
    BlockBodyV2,
    DeployHashes,
    TransferHashes,
    Proposer,
    BlockMetadata,
    Deploy,
    DeployMetadata,
    Transfer,
    StateStore,
    FinalizedApprovals,
}

impl std::fmt::Display for Databases {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BlockHeader => write!(f, "block_header"),
            Self::BlockBodyV1 => write!(f, "block_body"),
            Self::BlockBodyV2 => write!(f, "block_body_merkle"),
            Self::DeployHashes => write!(f, "deploy_hashes"),
            Self::TransferHashes => write!(f, "transfer_hashes"),
            Self::Proposer => write!(f, "proposers"),
            Self::BlockMetadata => write!(f, "block_metadata"),
            Self::Deploy => write!(f, "deploys"),
            Self::DeployMetadata => write!(f, "deploy_metadata"),
            Self::Transfer => write!(f, "transfer"),
            Self::StateStore => write!(f, "state_store"),
            Self::FinalizedApprovals => write!(f, "finalized_approvals"),
        }
    }
}

pub trait Database {
    fn db_name() -> &'static str;

    fn parse_element(bytes: &[u8]) -> Result<()>;

    fn parse_elements(mut cursor: RoCursor) -> Result<()> {
        for (_raw_key, raw_val) in cursor.iter() {
            Self::parse_element(raw_val)?;
        }
        Ok(())
    }

    fn check_db(env: &Environment) -> Result<()> {
        println!("Checking {} database.", Self::db_name());
        let txn = env.begin_ro_txn()?;
        let db = unsafe { txn.open_db(Some(Self::db_name()))? };
        if let Ok(cursor) = txn.open_ro_cursor(db) {
            Self::parse_elements(cursor)?;
        }
        Ok(())
    }
}
