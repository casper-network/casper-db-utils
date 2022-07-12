mod common;
mod logging;
mod subcommands;
#[cfg(test)]
pub(crate) mod test_utils;

use std::{fs::OpenOptions, process};

use clap::{crate_description, crate_version, Arg, Command};
use log::error;

use subcommands::{archive, check, latest_block_summary, trie_compact, unsparse};

const LOGGING: &str = "logging";

enum DisplayOrder {
    Archive,
    Check,
    LatestBlock,
    TrieCompact,
    Unsparse,
}

fn cli() -> Command<'static> {
    Command::new("casper-db-utils")
        .version(crate_version!())
        .about(crate_description!())
        .arg_required_else_help(true)
        .subcommand(archive::command(DisplayOrder::Archive as usize))
        .subcommand(check::command(DisplayOrder::Check as usize))
        .subcommand(latest_block_summary::command(
            DisplayOrder::LatestBlock as usize,
        ))
        .subcommand(trie_compact::command(DisplayOrder::TrieCompact as usize))
        .subcommand(unsparse::command(DisplayOrder::Unsparse as usize))
        .arg(
            Arg::new(LOGGING)
                .short('l')
                .long(LOGGING)
                .takes_value(true)
                .value_name("LOGFILE_PATH")
                .help("Path to file where program will dump log messages."),
        )
}

fn main() {
    let arg_matches = cli().get_matches();

    // Initialize logger.
    arg_matches.value_of(LOGGING).map_or_else(
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

    let (subcommand_name, matches) = arg_matches.subcommand().unwrap_or_else(|| {
        error!("{}", cli().get_long_about().unwrap());
        process::exit(1);
    });

    let succeeded = match subcommand_name {
        archive::COMMAND_NAME => archive::run(matches),
        check::COMMAND_NAME => check::run(matches),
        latest_block_summary::COMMAND_NAME => latest_block_summary::run(matches),
        trie_compact::COMMAND_NAME => trie_compact::run(matches),
        unsparse::COMMAND_NAME => unsparse::run(matches),
        _ => unreachable!("{} should be handled above", subcommand_name),
    };

    if !succeeded {
        process::exit(1);
    }
}
