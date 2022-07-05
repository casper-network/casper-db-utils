use std::process;

use clap::{ArgMatches, Command};
use thiserror::Error as ThisError;

pub use create::Error as CreateError;
pub use unpack::Error as UnpackError;

use super::Error as SubcommandError;

mod create;
mod ring_buffer;
mod tar_utils;
mod unpack;
mod zstd_utils;

pub const COMMAND_NAME: &str = "archive";

enum DisplayOrder {
    Create,
    Unpack,
}

#[derive(ThisError, Debug)]
pub enum Error {
    #[error("create: {0}")]
    Create(#[from] CreateError),
    #[error("unpack: {0}")]
    Unpack(#[from] UnpackError),
}

impl From<Error> for SubcommandError {
    fn from(err: Error) -> Self {
        match err {
            Error::Create(create_err) => SubcommandError::ArchiveCreate(create_err),
            Error::Unpack(unpack_err) => SubcommandError::ArchiveUnpack(unpack_err),
        }
    }
}

pub fn command(display_order: usize) -> Command<'static> {
    Command::new(COMMAND_NAME)
        .display_order(display_order)
        .about("Utilities for working with a compressed archive of a casper-node storage instance.")
        .subcommand(create::command(DisplayOrder::Create as usize))
        .subcommand(unpack::command(DisplayOrder::Unpack as usize))
}

pub fn run(matches: &ArgMatches) -> Result<(), Error> {
    let (subcommand_name, matches) = matches.subcommand().unwrap_or_else(|| {
        process::exit(1);
    });

    match subcommand_name {
        create::COMMAND_NAME => create::run(matches).map_err(Error::Create),
        unpack::COMMAND_NAME => unpack::run(matches).map_err(Error::Unpack),
        _ => unreachable!("{} should be handled above", subcommand_name),
    }
}
