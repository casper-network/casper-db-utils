mod db;
mod logging;

use std::{fs::OpenOptions, process::exit};

use clap::{Arg, Command};
use log::{error};

use db::{
    db_env, BlockBodyDatabase, BlockBodyMerkleDatabase, BlockHeaderDatabase, BlockMetadataDatabase,
    Database, DeployDatabase, DeployHashesDatabase, DeployMetadataDatabase,
    FinalizedApprovalsDatabase, ProposerDatabase, StateStoreDatabase, TransferDatabase,
    TransferHashesDatabase,
};

fn main() {
    let matches = Command::new("db-util")
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

    let path = matches.value_of("db-path").unwrap();
    let failfast = !matches.is_present("no-failfast");
    let env = db_env(path.into()).expect("failed to initialize DB environment");
    if let Some(db_name) = matches.value_of("specific") {
        let start_at: usize = matches
            .value_of("start-at")
            .unwrap()
            .parse()
            .expect("Value of \"--start-at\" must be an integer.");
        let res = match db_name.trim() {
            "block_body" => BlockBodyDatabase::check_db(&env, failfast, start_at),
            "block_body_merkle" => BlockBodyMerkleDatabase::check_db(&env, failfast, start_at),
            "block_header" => BlockHeaderDatabase::check_db(&env, failfast, start_at),
            "block_metadata" => BlockMetadataDatabase::check_db(&env, failfast, start_at),
            "deploy_hashes" => DeployHashesDatabase::check_db(&env, failfast, start_at),
            "deploy_metadata" => DeployMetadataDatabase::check_db(&env, failfast, start_at),
            "deploys" => DeployDatabase::check_db(&env, failfast, start_at),
            "finalized_approvals" => FinalizedApprovalsDatabase::check_db(&env, failfast, start_at),
            "proposers" => ProposerDatabase::check_db(&env, failfast, start_at),
            "state_store" => StateStoreDatabase::check_db(&env, failfast, start_at),
            "transfer" => TransferDatabase::check_db(&env, failfast, start_at),
            "transfer_hashes" => TransferHashesDatabase::check_db(&env, failfast, start_at),
            _ => panic!("Database {} not found.", db_name),
        };
        match res {
            Ok(()) => {
                exit(0);
            }
            Err(e) => {
                error!("Database {} check failed. {}", db_name, e);
                exit(128);
            }
        }
    } else {
        let start_at = 0;
        BlockBodyDatabase::check_db(&env, failfast, start_at).expect("Block Body DB check failed");
        BlockBodyMerkleDatabase::check_db(&env, failfast, start_at)
            .expect("Block Body Merkle DB check failed");
        BlockHeaderDatabase::check_db(&env, failfast, start_at)
            .expect("Block Header DB check failed");
        BlockMetadataDatabase::check_db(&env, failfast, start_at)
            .expect("Block Metadata DB check failed");
        DeployHashesDatabase::check_db(&env, failfast, start_at)
            .expect("Deploy Hashes DB check failed");
        DeployMetadataDatabase::check_db(&env, failfast, start_at)
            .expect("Deploy Metadata DB check failed");
        DeployDatabase::check_db(&env, failfast, start_at).expect("Deploy DB check failed");
        FinalizedApprovalsDatabase::check_db(&env, failfast, start_at)
            .expect("Finalized Approvals DB check failed");
        ProposerDatabase::check_db(&env, failfast, start_at).expect("Proposer DB check failed");
        StateStoreDatabase::check_db(&env, failfast, start_at)
            .expect("State Store DB check failed");
        TransferDatabase::check_db(&env, failfast, start_at).expect("Transfer DB check failed");
        TransferHashesDatabase::check_db(&env, failfast, start_at)
            .expect("Transfer Hashes DB check failed");
    }
}
