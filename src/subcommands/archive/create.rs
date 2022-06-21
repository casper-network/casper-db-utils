mod pack;
#[cfg(test)]
mod tests;

use std::io::Error as IoError;

use clap::{Arg, ArgMatches, Command};
use log::error;
use thiserror::Error as ThisError;

use super::zstd_utils::Error as ZstdError;

pub const COMMAND_NAME: &str = "create";
const NO_CHECKSUMS: &str = "no-checksums";
const OUTPUT: &str = "output";
const DB: &str = "db";

#[derive(Debug, ThisError)]
pub enum Error {
    #[error("Error creating destination archive file: {0}")]
    Destination(IoError),
    #[error("Error streaming from tarball to zstd encoder: {0}")]
    Streaming(IoError),
    #[error("Error packing tarball: {0}")]
    Tar(IoError),
    #[error("Zstd error: {0}")]
    ZstdEncoderSetup(#[from] ZstdError),
}

enum DisplayOrder {
    Db,
    Output,
    NoChecksums,
}

pub fn command(display_order: usize) -> Command<'static> {
    Command::new(COMMAND_NAME)
        .display_order(display_order)
        .about(
            "Packs a casper-node storage instance to a tarball and then compresses it with ZSTD.",
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
                .help("Output file path for the compressed TAR archive."),
        )
        .arg(
            Arg::new(NO_CHECKSUMS)
                .display_order(DisplayOrder::NoChecksums as usize)
                .long(NO_CHECKSUMS)
                .takes_value(false)
                .help("Disable frame checksums on zstd encoding."),
        )
}

pub fn run(matches: &ArgMatches) -> bool {
    let db_path = matches.value_of(DB).unwrap();
    let dest = matches.value_of(OUTPUT).unwrap();
    let require_checksums = !matches.is_present(NO_CHECKSUMS);
    let result = pack::create_archive(db_path.into(), dest.into(), require_checksums);

    if let Err(error) = &result {
        error!("Archive packing failed. {}", error);
    }

    result.is_ok()
}
