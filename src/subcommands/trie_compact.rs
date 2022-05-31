mod compact;
mod helpers;
#[cfg(test)]
mod tests;
// All code in the `utils` mod was copied from `casper-node` because it isn't available in the
// public interface.
mod utils;

use clap::{Arg, ArgMatches, Command};
use log::error;

use compact::DestinationOptions;

pub const COMMAND_NAME: &str = "compact-trie";
const APPEND: &str = "append";
const DESTINATION_TRIE_STORE_PATH: &str = "dest-trie";
const OVERWRITE: &str = "overwrite";
const MAX_DB_SIZE: &str = "max-db-size";
const DEFAULT_MAX_DB_SIZE: &str = "483183820800"; // 450 gb
const SOURCE_TRIE_STORE_PATH: &str = "src-trie";
const STORAGE_PATH: &str = "storage-path";

pub fn command() -> Command<'static> {
    Command::new(COMMAND_NAME)
        .about("Writes a compacted version of the block entries in the source trie store to the destination.")
        .arg(
            Arg::new(APPEND)
                .required(false)
                .short('a')
                .long(APPEND)
                .takes_value(false)
                .conflicts_with(OVERWRITE)
                .help("Append output to an already existing output `data.lmdb` file in destination directory."),
        )
        .arg(
            Arg::new(DESTINATION_TRIE_STORE_PATH)
                .required(true)
                .short('d')
                .long(DESTINATION_TRIE_STORE_PATH)
                .takes_value(true)
                .value_name("DESTINATION_TRIE_STORE_DIR_PATH")
                .help("Path of the directory where the output `data.lmdb` file will be created."),
        )
        .arg(
            Arg::new(OVERWRITE)
                .required(false)
                .short('w')
                .long(OVERWRITE)
                .takes_value(false)
                .conflicts_with(APPEND)
                .help("Overwrite an already existing output `data.lmdb` file in destination directory."),
        )
        .arg(
            Arg::new(MAX_DB_SIZE)
                .required(false)
                .short('m')
                .long(MAX_DB_SIZE)
                .takes_value(true)
                .default_value(DEFAULT_MAX_DB_SIZE)
                .value_name("MAX_DB_SIZE")
                .help("Maximum size the DB files are allowed to be, in bytes."),
        )
        .arg(
            Arg::new(SOURCE_TRIE_STORE_PATH)
                .required(true)
                .short('s')
                .long(SOURCE_TRIE_STORE_PATH)
                .takes_value(true)
                .value_name("SOURCE_TRIE_STORE_DIR_PATH")
                .help("Path of the directory with the source `data.lmdb` file."),
        )
        .arg(
            Arg::new(STORAGE_PATH)
                .required(true)
                .short('b')
                .long(STORAGE_PATH)
                .takes_value(true)
                .value_name("STORAGE_DIR_PATH")
                .help("Path of the directory with the `storage.lmdb` file. Used to find all blocks' state root hashes."),
        )
}

pub fn run(matches: &ArgMatches) -> bool {
    let storage_path = matches.value_of(STORAGE_PATH).unwrap();
    let source_trie_path = matches.value_of(SOURCE_TRIE_STORE_PATH).unwrap();
    let destination_trie_path = matches.value_of(DESTINATION_TRIE_STORE_PATH).unwrap();
    // Prettier than C style if/else.
    let dest_opt = match matches {
        _ if matches.is_present(APPEND) => DestinationOptions::Append,
        _ if matches.is_present(OVERWRITE) => DestinationOptions::Overwrite,
        _ => DestinationOptions::New,
    };
    let max_db_size = matches
        .value_of(MAX_DB_SIZE)
        .unwrap()
        .parse()
        .expect("Value of \"--max-db-size\" must be an integer.");

    let result = compact::trie_compact(
        storage_path.into(),
        source_trie_path.into(),
        destination_trie_path.into(),
        dest_opt,
        max_db_size,
    );

    if let Err(error) = &result {
        error!("Trie compact failed. {}", error);
    }

    result.is_ok()
}
