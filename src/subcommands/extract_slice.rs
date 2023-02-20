mod db_helpers;
mod extract;
mod global_state;
mod storage;
#[cfg(test)]
mod tests;

use std::{io::Error as IoError, path::Path};

use bincode::Error as BincodeError;
use casper_hashing::Digest;
use casper_node::types::BlockHash;
use clap::{Arg, ArgMatches, Command};
use lmdb::Error as LmdbError;
use thiserror::Error as ThisError;

pub const COMMAND_NAME: &str = "extract-slice";
const BLOCK_HASH: &str = "block-hash";
const OUTPUT: &str = "output";
const SOURCE_DB_PATH: &str = "source-db-path";

/// Errors encountered when running the `extract-slice` subcommand.
#[derive(Debug, ThisError)]
pub enum Error {
    #[error("Error (de)serializing items with bincode: {0}")]
    Bincode(#[from] BincodeError),
    #[error("Error creating the destination execution engine: {0}")]
    CreateExecutionEngine(anyhow::Error),
    #[error("Error operating the database: {0}")]
    Database(#[from] LmdbError),
    #[error("Error loading the source execution engine: {0}")]
    LoadExecutionEngine(anyhow::Error),
    #[error("Error writing output: {0}")]
    Output(#[from] IoError),
    #[error("Error parsing element for block hash {0} in {1} DB: {2}")]
    Parsing(BlockHash, String, BincodeError),
    #[error("Error transferring state root: {0}")]
    StateRootTransfer(anyhow::Error),
}

enum DisplayOrder {
    SourceDbPath,
    Output,
    BlockHash,
}

pub fn command(display_order: usize) -> Command<'static> {
    Command::new(COMMAND_NAME)
        .display_order(display_order)
        .about(
            "Reads all data for a given block hash (block, deploys, execution \
                results, global state) from a storage directory and stores \
                them to a new directory in two LMDB files",
        )
        .arg(
            Arg::new(SOURCE_DB_PATH)
                .display_order(DisplayOrder::SourceDbPath as usize)
                .required(true)
                .short('d')
                .long(SOURCE_DB_PATH)
                .takes_value(true)
                .value_name("SOURCE_DB_PATH")
                .help(
                    "Path of the directory with the `storage.lmdb` and \
                `data.lmdb` files.",
                ),
        )
        .arg(
            Arg::new(OUTPUT)
                .display_order(DisplayOrder::Output as usize)
                .short('o')
                .long(OUTPUT)
                .takes_value(true)
                .value_name("OUTPUT_DB_PATH")
                .help(
                    "Path of the directory where the program will output the \
                    two newly created `storage.lmdb` and `data.lmdb` files. \
                    The directory must not exist when running this command.",
                ),
        )
        .arg(
            Arg::new(BLOCK_HASH)
                .display_order(DisplayOrder::BlockHash as usize)
                .short('b')
                .long(OUTPUT)
                .takes_value(true)
                .value_name("BLOCK_HASH")
                .help("Hash of the block which defines the slice."),
        )
}

pub fn run(matches: &ArgMatches) -> Result<(), Error> {
    let path = Path::new(
        matches
            .value_of(SOURCE_DB_PATH)
            .expect("should have db-path arg"),
    );
    let output = Path::new(matches.value_of(OUTPUT).expect("should have output arg"));
    let block_hash_string = matches
        .value_of(BLOCK_HASH)
        .expect("should have block-hash arg");
    let block_hash: BlockHash = Digest::from_hex(block_hash_string)
        .expect("should parse block hash to hex format")
        .into();
    extract::extract_slice(path, output, block_hash)
}
