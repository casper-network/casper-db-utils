mod pack;
#[cfg(test)]
mod tests;

use std::io::Error as IoError;

use clap::{Arg, ArgMatches, Command};
use log::error;
use thiserror::Error as ThisError;

use super::zstd_utils::Error as ZstdError;

pub const COMMAND_NAME: &str = "create";
const OUTPUT: &str = "output";
const DB: &str = "db-dir";

#[derive(Debug, ThisError)]
pub enum Error {
    #[error("Archiving contents into tarball failed")]
    ArchiveStream,
    #[error("Error creating destination archive file: {0}")]
    Destination(IoError),
    #[error("Error streaming from tarball to zstd encoder: {0}")]
    Streaming(IoError),
    #[error("Zstd error: {0}")]
    ZstdEncoderSetup(#[from] ZstdError),
}

enum DisplayOrder {
    Db,
    Output,
}

pub fn command(display_order: usize) -> Command<'static> {
    Command::new(COMMAND_NAME)
        .display_order(display_order)
        .about(
            "Packs a casper-node storage instance to a tarball and then compresses it with zstd.",
        )
        .arg(
            Arg::new(DB)
                .display_order(DisplayOrder::Db as usize)
                .required(true)
                .short('d')
                .long(DB)
                .takes_value(true)
                .value_name("DIR_PATH")
                .help("Path to the database directory."),
        )
        .arg(
            Arg::new(OUTPUT)
                .display_order(DisplayOrder::Output as usize)
                .required(true)
                .short('o')
                .long(OUTPUT)
                .takes_value(true)
                .value_name("FILE_PATH")
                .help("Output file path for the compressed tar archive."),
        )
}

pub fn run(matches: &ArgMatches) -> Result<(), Error> {
    let db_path = matches.value_of(DB).unwrap();
    let dest = matches.value_of(OUTPUT).unwrap();
    pack::create_archive(db_path, dest)
}
