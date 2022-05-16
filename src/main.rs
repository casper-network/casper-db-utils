mod check;
mod db;
mod logging;

use std::{fs::OpenOptions, process::exit};

use clap::{Arg, Command};
use log::error;

const CHECK: &str = "check";
const DB_PATH: &str = "db-path";
const LOGGING: &str = "logging";
const NO_FAILFAST: &str = "no-failfast";
const SPECIFIC: &str = "specific";
const START_AT: &str = "start-at";

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

    if let Some((CHECK, sub_m)) = matches.subcommand() {
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
            Err(e) => {
                error!("Database check failed. {}", e);
                exit(128);
            }
        }
    }
}
