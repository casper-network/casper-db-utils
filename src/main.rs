mod check;
mod db;
mod logging;

use std::{fs::OpenOptions, process::exit};

use clap::{Arg, Command};
use log::error;

fn main() {
    let matches = Command::new("casper-db-utils")
        .arg_required_else_help(true)
        .about("casper-node database utils.")
        .subcommand(
            Command::new("check")
                .about("Checks validity of entries in a storage database through ensuring deserialization is successful.")
                .arg(
                    Arg::new("no-failfast")
                        .short('f')
                        .long("no-failfast")
                        .takes_value(false)
                        .help(
                            "Program will not terminate when failing to parse an element in the database.",
                        ),
                )
                .arg(
                    Arg::new("db-path")
                        .required(true)
                        .short('d')
                        .long("db-path")
                        .takes_value(true)
                        .value_name("DB_PATH")
                        .help("Path to the storage.lmdb file."),
                )
                .arg(
                    Arg::new("specific")
                        .short('s')
                        .long("specific")
                        .takes_value(true)
                        .value_name("DB_NAME")
                        .help(
                            "Parse a specific database.",
                        ),
                )
                .arg(
                    Arg::new("start-at")
                        .short('i')
                        .long("start-at")
                        .takes_value(true)
                        .value_name("ENTRY_INDEX")
                        .requires("specific")
                        .default_value("0")
                        .help(
                            "Entry index from which parsing will start. Requires \"--specific\" parameter to be set.",
                        ),
                )
        )
        .arg(
            Arg::new("logging")
                .short('l')
                .long("logging")
                .takes_value(true)
                .value_name("LOGFILE_PATH")
                .help(
                    "Path to file where program will dump log messages.",
                ),
        )
        .get_matches();

    // Initialize logger.
    matches.value_of("logging").map_or_else(
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
        Some(("check", sub_m)) => {
            let path = sub_m.value_of("db-path").unwrap();
            let failfast = !sub_m.is_present("no-failfast");
            let specific = sub_m.value_of("specific");
            let start_at: usize = sub_m
                .value_of("start-at")
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
        _ => {}
    }
}
