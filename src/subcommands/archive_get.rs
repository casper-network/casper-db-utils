mod download_stream;
mod zstd_decode;

use std::io::Error as IoError;

use clap::{Arg, ArgMatches, Command};
use log::error;
use reqwest::Error as ReqwestError;
use thiserror::Error as ThisError;

pub const COMMAND_NAME: &str = "archive-get";
const URL: &str = "url";
const OUTPUT: &str = "output";
const EXTRACT: &str = "extract";
const WINDOW_LOG_DISTANCE: &str = "window-log-distance";

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
    #[error("Error setting up zstd decoder: {0}")]
    ZstdDecoderSetup(IoError),
}

enum DisplayOrder {
    Url,
    Output,
    Extract,
    WindowLogDistance,
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
                .value_name("OUT_DIR")
                .help("Output file path for the decompressed TAR archive."),
        )
        .arg(
            Arg::new(EXTRACT)
                .display_order(DisplayOrder::Extract as usize)
                .short('x')
                .long(EXTRACT)
                .help("Stream the downloaded data into a zstd decoder to output the extracted archive."),
        )
        .arg(
            Arg::new(WINDOW_LOG_DISTANCE)
                .display_order(DisplayOrder::WindowLogDistance as usize)
                .required(true)
                .short('w')
                .long(WINDOW_LOG_DISTANCE)
                .takes_value(true)
                .value_name("WINDOW_LOG_DISTANCE")
                .help("Window log size passed to the zstd decoder as the \"--long\" parameter."),
        )
}

pub fn run(matches: &ArgMatches) -> bool {
    let url = matches.value_of(URL).unwrap();
    let dest = matches.value_of(OUTPUT).unwrap();
    let zstd_decode = matches.is_present(EXTRACT);
    let log_distance = matches
        .value_of(WINDOW_LOG_DISTANCE)
        .map(|log_distance_str| {
            log_distance_str
                .parse()
                .expect("Window log distance must be an integer.")
        });
    let result = download_stream::download_archive(url, dest.into(), zstd_decode, log_distance);

    if let Err(error) = &result {
        error!("Archive get failed. {}", error);
    }

    result.is_ok()
}
