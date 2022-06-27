use std::process;

use clap::{ArgMatches, Command};

mod create;
mod ring_buffer;
mod tar_utils;
mod unpack;
mod zstd_utils;

pub const COMMAND_NAME: &str = "archive";

enum DisplayOrder {
    Create,
    Unpack,
}

pub fn command(display_order: usize) -> Command<'static> {
    Command::new(COMMAND_NAME)
        .display_order(display_order)
        .about("Utilities for working with a compressed archive of a casper-node storage instance.")
        .subcommand(create::command(DisplayOrder::Create as usize))
        .subcommand(unpack::command(DisplayOrder::Unpack as usize))
}

pub fn run(matches: &ArgMatches) -> bool {
    let (subcommand_name, matches) = matches.subcommand().unwrap_or_else(|| {
        process::exit(1);
    });

    match subcommand_name {
        create::COMMAND_NAME => create::run(matches),
        unpack::COMMAND_NAME => unpack::run(matches),
        _ => unreachable!("{} should be handled above", subcommand_name),
    }
}
