mod block_info;
mod read_db;
#[cfg(test)]
mod tests;

use std::path::Path;

use clap::{Arg, ArgMatches, Command};
use log::error;

pub const COMMAND_NAME: &str = "latest-block";
const DB_PATH: &str = "db-path";
const OUTPUT: &str = "output";

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

pub fn run(matches: &ArgMatches) -> bool {
    let path = Path::new(matches.value_of(DB_PATH).expect("should have db-path arg"));
    let output = matches.value_of(OUTPUT).map(Path::new);
    let result = read_db::latest_block(path, output);

    if let Err(error) = &result {
        error!("Latest block failed. {}", error);
    }

    result.is_ok()
}
