mod block_info;
mod read_db;
#[cfg(test)]
mod tests;

use std::{io::Error as IoError, path::Path};

use bincode::Error as BincodeError;
use clap::{Arg, ArgMatches, Command};
use lmdb::Error as LmdbError;
use serde_json::Error as SerializationError;
use thiserror::Error as ThisError;

pub const COMMAND_NAME: &str = "latest-block-summary";
const DB_PATH: &str = "db-path";
const OUTPUT: &str = "output";

/// Errors encountered when operating on the storage database.
#[derive(Debug, ThisError)]
pub enum Error {
    #[error("No blocks found in the block header database")]
    EmptyDatabase,
    /// Parsing error on entry at index in the database.
    #[error("Error parsing element {0}: {1}")]
    Parsing(usize, BincodeError),
    /// Database operation error.
    #[error("Error operating the database: {0}")]
    Database(#[from] LmdbError),
    #[error("Error serializing output: {0}")]
    Serialize(#[from] SerializationError),
    #[error("Error writing output: {0}")]
    Output(#[from] IoError),
}

enum DisplayOrder {
    DbPath,
    Output,
}

pub fn command(display_order: usize) -> Command<'static> {
    Command::new(COMMAND_NAME)
        .display_order(display_order)
        .about(
            "Outputs information about the latest block in a storage database \
            in JSON format.",
        )
        .arg(
            Arg::new(DB_PATH)
                .display_order(DisplayOrder::DbPath as usize)
                .required(true)
                .short('d')
                .long(DB_PATH)
                .takes_value(true)
                .value_name("DB_PATH")
                .help("Path to the storage.lmdb file."),
        )
        .arg(
            Arg::new(OUTPUT)
                .display_order(DisplayOrder::Output as usize)
                .short('o')
                .long(OUTPUT)
                .takes_value(true)
                .value_name("FILE_PATH")
                .help(
                    "Path to where the program will output the metadata. \
                    If unspecified, defaults to standard output.",
                ),
        )
}

pub fn run(matches: &ArgMatches) -> Result<(), Error> {
    let path = Path::new(matches.value_of(DB_PATH).expect("should have db-path arg"));
    let output = matches.value_of(OUTPUT).map(Path::new);
    read_db::latest_block_summary(path, output)
}
