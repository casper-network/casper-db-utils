mod download_stream;
mod file_stream;
#[cfg(test)]
mod tests;

use std::{
    io::Error as IoError,
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

fn unpack<P: AsRef<Path>>(input: Input, dest: P) -> Result<(), Error> {
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
                .group(INPUT_SOURCE)
                .short('u')
                .long(URL)
                .takes_value(true)
                .value_name("URL")
                .help("URL of the compressed archive."),
        )
        .arg(
            Arg::new(FILE)
                .display_order(DisplayOrder::File as usize)
                .group(INPUT_SOURCE)
                .required(true)
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

pub fn run(matches: &ArgMatches) -> bool {
    let input = matches
        .value_of(URL)
        .map(|url| Input::Url(url.to_string()))
        .unwrap_or_else(|| {
            matches
                .value_of(FILE)
                .map(|path| Input::File(path.into()))
                .unwrap_or_else(|| panic!("Should have one of {} or {}", FILE, URL))
        });
    let dest = matches.value_of(OUTPUT).unwrap();
    let result = unpack(input, dest);

    if let Err(error) = &result {
        error!("Archive unpack failed. {}", error);
    }

    result.is_ok()
}
