mod download_stream;
#[cfg(test)]
mod tests;
mod zstd_decode;

use std::{io::Error as IoError, path::PathBuf};

use clap::{Arg, ArgMatches, Command};
use log::{error, warn};
use reqwest::Error as ReqwestError;
use thiserror::Error as ThisError;

use super::tar_utils;

pub const COMMAND_NAME: &str = "unpack";
const URL: &str = "url";
const OUTPUT: &str = "output";

#[derive(Debug, ThisError)]
pub enum Error {
    #[error("HTTP request error: {0}")]
    Request(#[from] ReqwestError),
    #[error("Error streaming from zstd decoder to destination file: {0}")]
    Streaming(IoError),
    #[error("Error creating destination archive file: {0}")]
    Destination(IoError),
    #[error("Error creating tokio runtime: {0}")]
    Runtime(IoError),
    #[error("Error unpacking tarball: {0}")]
    Tar(IoError),
    #[error("Error setting up zstd decoder: {0}")]
    ZstdDecoderSetup(IoError),
}

enum DisplayOrder {
    Url,
    Output,
}

fn unpack(url: &str, dest: PathBuf) -> Result<(), Error> {
    let archive_path = dest.as_path().join("casper_db_archive.tar.zst");
    download_stream::download_archive(url, archive_path.clone())?;
    tar_utils::unarchive(archive_path.clone(), dest).map_err(Error::Tar)?;
    if let Err(io_err) = std::fs::remove_file(archive_path.clone()) {
        warn!(
            "Couldn't remove tarball at {} after unpacking: {}",
            archive_path.as_os_str().to_string_lossy(),
            io_err
        );
    }
    Ok(())
}

pub fn command(display_order: usize) -> Command<'static> {
    Command::new(COMMAND_NAME)
        .display_order(display_order)
        .about("Downloads and decompresses a ZSTD TAR archive of a casper-node storage instance.")
        .arg(
            Arg::new(URL)
                .display_order(DisplayOrder::Url as usize)
                .required(true)
                .short('u')
                .long(URL)
                .takes_value(true)
                .value_name("URL")
                .help("URL of the compressed archive."),
        )
        .arg(
            Arg::new(OUTPUT)
                .display_order(DisplayOrder::Output as usize)
                .required(true)
                .short('o')
                .long(OUTPUT)
                .takes_value(true)
                .value_name("FILE_PATH")
                .help("Output file path for the decompressed TAR archive."),
        )
}

pub fn run(matches: &ArgMatches) -> bool {
    let url = matches.value_of(URL).unwrap();
    let dest = matches.value_of(OUTPUT).unwrap();
    let result = unpack(url, dest.into());

    if let Err(error) = &result {
        error!("Archive unpack failed. {}", error);
    }

    result.is_ok()
}
