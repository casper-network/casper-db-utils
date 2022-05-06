use std::path::PathBuf;

use lmdb::{Cursor, Environment, EnvironmentFlags, RoCursor, Transaction};
use thiserror::Error;

use casper_hashing::Digest;
use casper_node::types::{
    BlockBody, BlockHeader, BlockSignatures, Deploy, DeployMetadata, FinalizedApprovals,
};
use casper_types::{bytesrepr::FromBytes, DeployHash, PublicKey, Transfer};

#[derive(Debug, Error)]
pub enum Error {
    #[error("failed database operation")]
    LmbdError(#[from] lmdb::Error),
    #[error("failed parsing struct with bincode")]
    BincodeError(#[from] bincode::Error),
    #[error("failed parsing struct with bytesrepr")]
    BytesreprError,
}

type Result<T> = std::result::Result<T, Error>;

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

impl Databases {
    pub fn check_db(&self, env: &Environment) -> Result<()> {
        println!("Checking {} database.", self);
        let txn = env.begin_ro_txn()?;
        let db = unsafe { txn.open_db(Some(&self.to_string()))? };
        if let Ok(cursor) = txn.open_ro_cursor(db) {
            match self {
                Self::BlockHeader => Self::check_block_header_db(cursor),
                Self::BlockBodyV1 => Self::check_block_body_v1_db(cursor),
                Self::BlockBodyV2 => Self::check_block_body_v2_db(cursor),
                Self::DeployHashes => Self::check_deploy_hashes_db(cursor),
                Self::TransferHashes => Self::check_transfer_hashes_db(cursor),
                Self::Proposer => Self::check_proposer_db(cursor),
                Self::BlockMetadata => Self::check_block_metadata_db(cursor),
                Self::Deploy => Self::check_deploy_db(cursor),
                Self::DeployMetadata => Self::check_deploy_metadata_db(cursor),
                Self::Transfer => Self::check_transfer_db(cursor),
                Self::StateStore => Self::check_state_store_db(cursor),
                Self::FinalizedApprovals => Self::check_finalized_approvals_db(cursor),
            }?;
        }
        txn.commit()?;
        Ok(())
    }

    fn check_block_header_db(mut cursor: RoCursor) -> Result<()> {
        for (_raw_key, raw_val) in cursor.iter() {
            let _: BlockHeader = bincode::deserialize(raw_val)?;
        }
        Ok(())
    }

    fn check_block_body_v1_db(mut cursor: RoCursor) -> Result<()> {
        for (_raw_key, raw_val) in cursor.iter() {
            let _: BlockBody = bincode::deserialize(raw_val)?;
        }
        Ok(())
    }

    fn check_block_body_v2_db(mut cursor: RoCursor) -> Result<()> {
        for (_raw_key, raw_val) in cursor.iter() {
            let _: (Digest, Digest) = FromBytes::from_bytes(raw_val)
                .map_err(|_| Error::BytesreprError)?
                .0;
        }
        Ok(())
    }

    fn check_deploy_hashes_db(mut cursor: RoCursor) -> Result<()> {
        for (_raw_key, raw_val) in cursor.iter() {
            let _: Vec<DeployHash> = FromBytes::from_bytes(raw_val)
                .map_err(|_| Error::BytesreprError)?
                .0;
        }
        Ok(())
    }

    fn check_transfer_hashes_db(mut cursor: RoCursor) -> Result<()> {
        for (_raw_key, raw_val) in cursor.iter() {
            let _: Vec<DeployHash> = FromBytes::from_bytes(raw_val)
                .map_err(|_| Error::BytesreprError)?
                .0;
        }
        Ok(())
    }

    fn check_proposer_db(mut cursor: RoCursor) -> Result<()> {
        for (_raw_key, raw_val) in cursor.iter() {
            let _: PublicKey = FromBytes::from_bytes(raw_val)
                .map_err(|_| Error::BytesreprError)?
                .0;
        }
        Ok(())
    }

    fn check_block_metadata_db(mut cursor: RoCursor) -> Result<()> {
        for (_raw_key, raw_val) in cursor.iter() {
            let _: BlockSignatures = bincode::deserialize(raw_val)?;
        }
        Ok(())
    }

    fn check_deploy_db(mut cursor: RoCursor) -> Result<()> {
        for (_raw_key, raw_val) in cursor.iter() {
            let _: Deploy = bincode::deserialize(raw_val)?;
        }
        Ok(())
    }

    fn check_deploy_metadata_db(mut cursor: RoCursor) -> Result<()> {
        for (_raw_key, raw_val) in cursor.iter() {
            let _: DeployMetadata = bincode::deserialize(raw_val)?;
        }
        Ok(())
    }

    fn check_transfer_db(mut cursor: RoCursor) -> Result<()> {
        for (_raw_key, raw_val) in cursor.iter() {
            let _: Vec<Transfer> = bincode::deserialize(raw_val)?;
        }
        Ok(())
    }

    fn check_state_store_db(mut cursor: RoCursor) -> Result<()> {
        for (_raw_key, raw_val) in cursor.iter() {
            let _: u64 = FromBytes::from_bytes(raw_val)
                .map_err(|_| Error::BytesreprError)?
                .0;
        }
        Ok(())
    }

    fn check_finalized_approvals_db(mut cursor: RoCursor) -> Result<()> {
        for (_raw_key, raw_val) in cursor.iter() {
            let _: FinalizedApprovals = bincode::deserialize(raw_val)?;
        }
        Ok(())
    }
}
