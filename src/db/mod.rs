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
#[cfg(test)]
mod tests;
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

use std::{io::Write, path::PathBuf, result::Result};

use lmdb::{Cursor, Environment, EnvironmentFlags, RoCursor, Transaction};
use log::info;
use thiserror::Error;

const ENTRY_LOG_INTERVAL: usize = 100_000;

#[derive(Debug, Error)]
pub enum DeserializationError {
    #[error("failed parsing struct with bincode")]
    BincodeError(#[from] bincode::Error),
    #[error("failed parsing struct with bytesrepr")]
    BytesreprError,
}

#[derive(Debug, Error)]
pub enum Error {
    Cumulated(Vec<Self>),
    Parsing(usize, DeserializationError),
    Database(#[from] lmdb::Error),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Database(e) => write!(f, "Error operating the database: {}", e),
            Self::Parsing(idx, inner) => write!(f, "Error parsing element {}: {}", idx, inner),
            Self::Cumulated(v) => {
                writeln!(f, "Errors caught:")?;
                for e in v {
                    writeln!(f, "{}", e)?;
                }
                Ok(())
            }
        }
    }
}

pub fn db_env(path: PathBuf) -> Result<Environment, Error> {
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

pub trait Database {
    fn db_name() -> &'static str;

    fn parse_element(bytes: &[u8]) -> Result<(), DeserializationError>;

    // TODO: Implement iterating with closure.
    // TODO: Use log crate.
    fn parse_elements(mut cursor: RoCursor, failfast: bool) -> Result<(), Error> {
        let mut stdout = std::io::stdout();
        // let mut error_buffer = String::new();
        let mut error_buffer = vec![];
        for (idx, (_raw_key, raw_val)) in cursor.iter().enumerate() {
            if let Err(e) =
                Self::parse_element(raw_val).map_err(|parsing_err| Error::Parsing(idx, parsing_err))
            {
                if failfast {
                    return Err(e);
                } else {
                    // error_buffer.push_str(&format!("{} database: {}\n", Self::db_name(), e));
                    error_buffer.push(e);
                }
            }
            if idx % ENTRY_LOG_INTERVAL == 0 {
                info!("Parsed {} entries...", idx);
                let _ = stdout.flush();
            }
        }
        info!("Parsing complete.");
        if !failfast && !error_buffer.is_empty() {
            // info!("Errors:\n{}", error_buffer);
            return Err(Error::Cumulated(error_buffer));
        }
        Ok(())
    }

    // TODO: Use log crate.
    fn parse_elements_starting_with(
        mut cursor: RoCursor,
        failfast: bool,
        start_at: usize,
    ) -> Result<(), Error> {
        info!("Skipping {} entries.", start_at);
        let mut stdout = std::io::stdout();
        // let mut error_buffer = String::new();
        let mut error_buffer = vec![];
        for (idx, (_raw_key, raw_val)) in cursor.iter().skip(start_at).enumerate() {
            if let Err(e) =
                Self::parse_element(raw_val).map_err(|parsing_err| Error::Parsing(idx, parsing_err))
            {
                if failfast {
                    return Err(e);
                } else {
                    // error_buffer.push_str(&format!("{} database: {}\n", Self::db_name(), e));
                    error_buffer.push(e);
                }
            }
            if idx % ENTRY_LOG_INTERVAL == 0 {
                info!("Parsed {} entries...", idx);
                let _ = stdout.flush();
            }
        }
        info!("Parsing complete.");
        // TODO: Move this to a log print instead of stdout.
        // Because this prints after iterating through all entries,
        // there is a chance the process exits without printing this,
        // therefore we need a separate logger.
        if !failfast && !error_buffer.is_empty() {
            // info!("Errors:\n{}", error_buffer);
            return Err(Error::Cumulated(error_buffer));
        }
        Ok(())
    }

    // TODO: Use log crate.
    fn check_db(env: &Environment, failfast: bool, start_at: usize) -> Result<(), Error> {
        // use lmdb_sys::{mdb_stat, MDB_stat, MDB_SUCCESS};
        // struct Stat(pub MDB_stat);

        info!("Checking {} database.", Self::db_name());
        let txn = env.begin_ro_txn()?;
        let db = unsafe { txn.open_db(Some(Self::db_name()))? };

        // let entries_count = unsafe {
        //     let mut stat = Stat(std::mem::zeroed());
        //     assert_eq!(mdb_stat(txn.txn(), db.dbi(), &mut stat.0), MDB_SUCCESS);
        //     stat.0.ms_entries
        // };
        // info!("GOT {} ENTRIES", entries_count);

        if let Ok(cursor) = txn.open_ro_cursor(db) {
            if start_at > 0 {
                Self::parse_elements_starting_with(cursor, failfast, start_at)?;
            } else {
                Self::parse_elements(cursor, failfast)?;
            }
        }
        Ok(())
    }
}
