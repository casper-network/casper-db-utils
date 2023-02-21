mod download_stream;
mod file_stream;
#[cfg(test)]
mod tests;

use std::{
    fs,
    io::{Error as IoError, ErrorKind},
    path::{Path, PathBuf},
};

use clap::{Arg, ArgGroup, ArgMatches, Command};
use log::error;
use reqwest::Error as ReqwestError;
use thiserror::Error as ThisError;

use super::zstd_utils::Error as ZstdError;

pub const COMMAND_NAME: &str = "unpack";
const FILE: &str = "file";
const INPUT_SOURCE: &str = "input-source";
const OUTPUT: &str = "output";
const URL: &str = "url";

#[derive(Debug, ThisError)]
pub enum Error {
    #[error("Error validating destination directory: {0}")]
    Destination(IoError),
    #[error("HTTP request error: {0}")]
    Request(#[from] ReqwestError),
    #[error("Error creating tokio runtime: {0}")]
    Runtime(IoError),
    #[error("Error reading source archive file: {0}")]
    Source(IoError),
    #[error("Error streaming from zstd decoder to destination file: {0}")]
    Streaming(IoError),
    #[error("Zstd error: {0}")]
    ZstdDecoderSetup(#[from] ZstdError),
}

enum DisplayOrder {
    Url,
    File,
    Output,
}

enum Input {
    File(PathBuf),
    Url(String),
}

fn validate_destination_path<P: AsRef<Path>>(path: P) -> Result<(), Error> {
    let path_ref = path.as_ref();
    if path_ref.exists() {
        if path_ref.is_dir() {
            if path_ref
                .read_dir()
                .map_err(Error::Destination)?
                .any(|entry| entry.is_ok())
            {
                Err(Error::Destination(IoError::new(
                    ErrorKind::InvalidInput,
                    "not an empty directory",
                )))
            } else {
                Ok(())
            }
        } else {
            Err(Error::Destination(IoError::new(
                ErrorKind::InvalidInput,
                "not a directory",
            )))
        }
    } else {
        fs::create_dir_all(path_ref).map_err(Error::Destination)
    }
}

fn unpack<P: AsRef<Path>>(input: Input, dest: P) -> Result<(), Error> {
    validate_destination_path(&dest)?;
    match input {
        Input::Url(url) => download_stream::download_and_unpack_archive(&url, dest),
        Input::File(path) => file_stream::file_stream_and_unpack_archive(path, dest),
    }
}

pub fn command(display_order: usize) -> Command<'static> {
    Command::new(COMMAND_NAME)
        .display_order(display_order)
        .about("Downloads and decompresses a zstd tar archive of a casper-node storage instance.")
        .arg(
            Arg::new(URL)
                .display_order(DisplayOrder::Url as usize)
                .short('u')
                .long(URL)
                .takes_value(true)
                .value_name("URL")
                .help("URL of the compressed archive."),
        )
        .arg(
            Arg::new(FILE)
                .display_order(DisplayOrder::File as usize)
                .short('f')
                .long(FILE)
                .takes_value(true)
                .value_name("FILE_PATH")
                .help("Path to the compressed archive."),
        )
        .arg(
            Arg::new(OUTPUT)
                .display_order(DisplayOrder::Output as usize)
                .required(true)
                .short('o')
                .long(OUTPUT)
                .takes_value(true)
                .value_name("DIR_PATH")
                .help(
                    "Path of the output directory for the decompressed \
                    tar archive contents. If the directory doesn't exist, \
                    it will be created along with any missing parent \
                    directories.",
                ),
        )
        .group(
            ArgGroup::new(INPUT_SOURCE)
                .required(true)
                .args(&[URL, FILE]),
        )
}

pub fn run(matches: &ArgMatches) -> Result<(), Error> {
    let input = matches
        .value_of(URL)
        .map(|url| Input::Url(url.to_string()))
        .unwrap_or_else(|| {
            matches
                .value_of(FILE)
                .map(|path| Input::File(path.into()))
                .unwrap_or_else(|| panic!("Should have one of {FILE} or {URL}"))
        });
    let dest = matches.value_of(OUTPUT).unwrap();
    unpack(input, dest)
}
