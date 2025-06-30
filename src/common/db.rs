pub(crate) mod blocks_index;
pub(crate) mod databases;
mod state_store_db;
#[cfg(test)]
mod tests;
mod versioned_database;

use bincode::Error as BincodeError;
use casper_types::bytesrepr;
use casper_types::bytesrepr::Error as BytesreprError;
use lmdb::Error as LmdbError;
use lmdb::Transaction as LmdbTransaction;
use lmdb::{Cursor, Environment, EnvironmentFlags, RoCursor};
use log::{error, info};
pub use state_store_db::StateStoreDatabase;
use std::path::Path;
use std::{
    fmt::{Display, Formatter, Result as FormatterResult},
    result::Result,
};
use thiserror::Error;
#[cfg(test)]
pub(crate) use versioned_database::MockData;
pub(crate) use versioned_database::VersionedDatabases;

pub const STORAGE_FILE_NAME: &str = "storage.lmdb";
pub const TRIE_STORE_FILE_NAME: &str = "data.lmdb";
const ENTRY_LOG_INTERVAL: usize = 100_000;
const MAX_DB_READERS: u32 = 100;

#[derive(Debug, Error)]
pub enum DeserializationError {
    #[error("failed parsing struct with bincode")]
    BincodeError(#[from] BincodeError),
    #[error("failed parsing struct with bytesrepr")]
    BytesreprError(String),
}

impl From<BytesreprError> for DeserializationError {
    fn from(error: BytesreprError) -> Self {
        Self::BytesreprError(error.to_string())
    }
}

/// Errors encountered when operating on the storage database.
#[derive(Debug, Error)]
pub enum Error {
    /// Errors accumulated when parsing a database with "--no-failfast".
    Accumulated(Vec<Self>),
    Parsing(usize, DeserializationError),
    /// Database operation error.
    Database(#[from] LmdbError),
    Bytesrepr(bytesrepr::Error),
    Bincode(bincode::Error),
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> FormatterResult {
        match self {
            Error::Database(e) => write!(f, "Error operating the database: {e}"),
            Error::Parsing(idx, inner) => write!(f, "Error parsing element {idx}: {inner}"),
            Error::Accumulated(accumulated_errors) => {
                writeln!(f, "Errors caught:")?;
                for error in accumulated_errors {
                    writeln!(f, "{error}")?;
                }
                Ok(())
            }
            Error::Bytesrepr(error) => write!(f, "Error when deserializing bytesrepr: {error}"),
            Error::Bincode(error_kind) => {
                write!(f, "Error when deserializing bincode: {error_kind}")
            }
        }
    }
}

pub fn db_env<P: AsRef<Path>>(path: P) -> Result<Environment, LmdbError> {
    let env = Environment::new()
        .set_flags(
            EnvironmentFlags::NO_SUB_DIR
                | EnvironmentFlags::NO_TLS
                | EnvironmentFlags::NO_READAHEAD,
        )
        .set_max_dbs(MAX_DB_READERS)
        .open(path.as_ref())?;
    Ok(env)
}

pub trait Database {
    fn db_name() -> &'static str;

    /// Parses a value of an entry in a database.
    fn parse_element(bytes: &[u8]) -> Result<(), DeserializationError>;

    /// Parses all elements of a database by trying to deserialize them sequentially.
    fn parse_elements(mut cursor: RoCursor, failfast: bool, start_at: usize) -> Result<(), Error> {
        if start_at > 0 {
            info!("Skipping {} entries.", start_at);
        }
        let mut error_buffer = vec![];
        for (idx, r) in cursor.iter().skip(start_at).enumerate() {
            if let Err(e) = r {
                if failfast {
                    return Err(Error::Database(e));
                } else {
                    error_buffer.push(Error::Database(e));
                    continue;
                }
            };
            let (_raw_key, raw_val) = r.unwrap();

            if let Err(e) =
                Self::parse_element(raw_val).map_err(|parsing_err| Error::Parsing(idx, parsing_err))
            {
                if failfast {
                    return Err(e);
                } else {
                    error_buffer.push(e);
                }
            }
            if idx % ENTRY_LOG_INTERVAL == 0 {
                info!("Parsed {} entries...", idx);
            }
        }
        info!("Parsing complete.");
        if !failfast && !error_buffer.is_empty() {
            return Err(Error::Accumulated(error_buffer));
        }
        Ok(())
    }

    /// Validates the database by ensuring every value of an entry can be parsed.
    fn check_db(env: &Environment, failfast: bool, start_at: usize) -> Result<(), Error> {
        info!("Checking {} database.", Self::db_name());
        let txn = env.begin_ro_txn()?;
        let db = unsafe { txn.open_db(Some(Self::db_name()))? };

        if let Ok(cursor) = txn.open_ro_cursor(db) {
            Self::parse_elements(cursor, failfast, start_at)?;
        }
        Ok(())
    }
}
