mod check;
mod db;
mod logging;
mod trie_compact;

use std::{fs::OpenOptions, process::exit};

use clap::{Arg, Command};
use log::error;

use trie_compact::{DestinationOptions, DEFAULT_MAX_DB_SIZE};

const CHECK: &str = "check";

const DB_PATH: &str = "db-path";
const NO_FAILFAST: &str = "no-failfast";
const SPECIFIC: &str = "specific";
const START_AT: &str = "start-at";

const COMPACT_TRIE: &str = "compact-trie";

const APPEND: &str = "append";
const DESTINATION_TRIE_STORE_PATH: &str = "dest-trie";
const OVERWRITE: &str = "overwrite";
const MAX_DB_SIZE: &str = "max-db-size";
const SOURCE_TRIE_STORE_PATH: &str = "src-trie";
const STORAGE_PATH: &str = "storage-path";

const LOGGING: &str = "logging";

fn main() {
    let matches = Command::new("casper-db-utils")
        .arg_required_else_help(true)
        .about("casper-node database utils.")
        .subcommand(
            Command::new(CHECK)
                .about("Checks validity of entries in a storage database through ensuring deserialization is successful.")
                .arg(
                    Arg::new(NO_FAILFAST)
                        .short('f')
                        .long(NO_FAILFAST)
                        .takes_value(false)
                        .help(
                            "Program will not terminate when failing to parse an element in the database.",
                        ),
                )
                .arg(
                    Arg::new(DB_PATH)
                        .required(true)
                        .short('d')
                        .long(DB_PATH)
                        .takes_value(true)
                        .value_name("DB_PATH")
                        .help("Path to the storage.lmdb file."),
                )
                .arg(
                    Arg::new(SPECIFIC)
                        .short('s')
                        .long(SPECIFIC)
                        .takes_value(true)
                        .value_name("DB_NAME")
                        .help(
                            "Parse a specific database.",
                        ),
                )
                .arg(
                    Arg::new(START_AT)
                        .short('i')
                        .long(START_AT)
                        .takes_value(true)
                        .value_name("ENTRY_INDEX")
                        .requires(SPECIFIC)
                        .default_value("0")
                        .help(
                            "Entry index from which parsing will start. Requires \"--specific\" parameter to be set.",
                        ),
                )
        )
        .subcommand(
            Command::new(COMPACT_TRIE)
                .about("Writes a compacted version of the block entries in the source trie store to the destination.")
                .arg(
                    Arg::new(APPEND)
                        .required(false)
                        .short('a')
                        .long(OVERWRITE)
                        .takes_value(false)
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
                        .help("Append output to an already existing output `data.lmdb` file in destination directory."),
                )
                .arg(
                    Arg::new(MAX_DB_SIZE)
                        .required(false)
                        .short('m')
                        .long(MAX_DB_SIZE)
                        .takes_value(true)
                        .default_value(&DEFAULT_MAX_DB_SIZE.to_string())
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
        )
        .arg(
            Arg::new(LOGGING)
                .short('l')
                .long(LOGGING)
                .takes_value(true)
                .value_name("LOGFILE_PATH")
                .help(
                    "Path to file where program will dump log messages.",
                ),
        )
        .get_matches();

    // Initialize logger.
    matches.value_of(LOGGING).map_or_else(
        || logging::init_term_logger().expect("Couldn't initialize terminal logger"),
        |path| {
            let logfile = OpenOptions::new()
                .append(true)
                .create(true)
                .open(path)
                .expect("Couldn't open logfile");
            let line_writer = std::io::LineWriter::new(logfile);
            logging::init_write_logger(line_writer).expect("Couldn't initialize logger to file");
        },
    );

    match matches.subcommand() {
        Some((CHECK, sub_m)) => {
            let path = sub_m.value_of(DB_PATH).unwrap();
            let failfast = !sub_m.is_present(NO_FAILFAST);
            let specific = sub_m.value_of(SPECIFIC);
            let start_at: usize = sub_m
                .value_of(START_AT)
                .unwrap()
                .parse()
                .expect("Value of \"--start-at\" must be an integer.");

            match check::check_db(path, failfast, specific, start_at) {
                Ok(()) => {
                    exit(0);
                }
                Err(err) => {
                    error!("Database check failed. {}", err);
                    exit(128);
                }
            }
        }
        Some((COMPACT_TRIE, sub_m)) => {
            let storage_path = sub_m.value_of(STORAGE_PATH).unwrap();
            let source_trie_path = sub_m.value_of(SOURCE_TRIE_STORE_PATH).unwrap();
            let destination_trie_path = sub_m.value_of(DESTINATION_TRIE_STORE_PATH).unwrap();
            // Prettier than C style if/else.
            let dest_opt = match sub_m {
                _ if sub_m.is_present(APPEND) => DestinationOptions::Append,
                _ if sub_m.is_present(OVERWRITE) => DestinationOptions::Overwrite,
                _ => DestinationOptions::New,
            };
            let max_db_size = sub_m
                .value_of(MAX_DB_SIZE)
                .unwrap()
                .parse()
                .expect("Value of \"--max-db-size\" must be an integer.");

            match trie_compact::trie_compact(
                storage_path.into(),
                source_trie_path.into(),
                destination_trie_path.into(),
                dest_opt,
                max_db_size,
            ) {
                Ok(()) => {
                    exit(0);
                }
                Err(err) => {
                    error!("Trie compact failed. {}", err);
                    exit(128);
                }
            }
        }
        _ => {
            error!("Error: invalid arguments. Run `casper-db-utils --help` for more information.");
            exit(129);
        }
    }
}
