mod remove;
#[cfg(test)]
mod tests;

use std::path::Path;

use bincode::Error as BincodeError;
use casper_hashing::Digest;
use casper_node::types::{BlockHash, DeployHash};
use clap::{Arg, ArgMatches, Command};
use lmdb::Error as LmdbError;
use thiserror::Error as ThisError;

pub const COMMAND_NAME: &str = "remove-block";
const BLOCK_HASH: &str = "block-hash";
const DB_PATH: &str = "db-path";

/// Errors encountered when operating on the storage database.
#[derive(Debug, ThisError)]
pub enum Error {
    /// Parsing error on entry in the block body database.
    #[error("Error parsing block body for block with hash {0}: {1}")]
    BodyParsing(BlockHash, BincodeError),
    /// Database operation error.
    #[error("Error operating the database: {0}")]
    Database(#[from] LmdbError),
    /// Parsing error on entry in the deploy metadata database.
    #[error("Error parsing execution results for block with hash {0} at deploy {1}: {2}")]
    ExecutionResultsParsing(BlockHash, DeployHash, BincodeError),
    /// Parsing error on entry in the block header database.
    #[error("Error parsing block header with hash {0}: {1}")]
    HeaderParsing(BlockHash, BincodeError),
    /// Missing entry in the deploy metadata database.
    #[error("Deploy with hash {0} not present in the database")]
    MissingDeploy(DeployHash),
    /// Missing entry in the block header database.
    #[error("Block header for block hash {0} not present in the database")]
    MissingHeader(BlockHash),
    /// Serialization error on entry in the deploy metadata database.
    #[error("Error serializing execution results for deploy {0}: {1}")]
    Serialization(DeployHash, BincodeError),
}

enum DisplayOrder {
    DbPath,
    BlockHash,
}

pub fn command(display_order: usize) -> Command<'static> {
    Command::new(COMMAND_NAME)
        .display_order(display_order)
        .about(
            "Removes the block header, body and execution results for a given \
            block hash from a storage database.",
        )
        .arg(
            Arg::new(DB_PATH)
                .display_order(DisplayOrder::DbPath as usize)
                .required(true)
                .short('d')
                .long(DB_PATH)
                .takes_value(true)
                .value_name("DB_PATH")
                .help("Path of the directory with the `storage.lmdb` file."),
        )
        .arg(
            Arg::new(BLOCK_HASH)
                .display_order(DisplayOrder::BlockHash as usize)
                .short('b')
                .long(BLOCK_HASH)
                .takes_value(true)
                .value_name("BLOCK_HASH")
                .help("Hash of the block to be removed."),
        )
}

pub fn run(matches: &ArgMatches) -> Result<(), Error> {
    let path = Path::new(matches.value_of(DB_PATH).expect("should have db-path arg"));
    let block_hash: BlockHash = matches
        .value_of(BLOCK_HASH)
        .map(|block_hash_str| {
            Digest::from_hex(block_hash_str)
                .expect("should parse block hash to hex format")
                .into()
        })
        .expect("should have block-hash arg");
    remove::remove_block(path, block_hash)
}
