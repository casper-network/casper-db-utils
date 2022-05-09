mod db;

use clap::{Arg, Command};

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
        .get_matches();

    let path = matches.value_of("db-path").unwrap();
    let failfast = !matches.is_present("no-failfast");
    let env = db_env(path.into()).expect("failed to initialize DB environment");
    if let Some(db_name) = matches.value_of("specific") {
        let start_at: usize = matches
            .value_of("start-at")
            .unwrap()
            .parse()
            .expect("Value of \"--start-at\" must be an integer.");
        match db_name.trim() {
            "block_body" => BlockBodyDatabase::check_db(&env, failfast, start_at)
                .expect("Block Body DB check failed"),
            "block_body_merkle" => BlockBodyMerkleDatabase::check_db(&env, failfast, start_at)
                .expect("Block Body Merkle DB check failed"),
            "block_header" => BlockHeaderDatabase::check_db(&env, failfast, start_at)
                .expect("Block Header DB check failed"),
            "block_metadata" => BlockMetadataDatabase::check_db(&env, failfast, start_at)
                .expect("Block Metadata DB check failed"),
            "deploy_hashes" => DeployHashesDatabase::check_db(&env, failfast, start_at)
                .expect("Deploy Hashes DB check failed"),
            "deploy_metadata" => DeployMetadataDatabase::check_db(&env, failfast, start_at)
                .expect("Deploy Metadata DB check failed"),
            "deploys" => {
                DeployDatabase::check_db(&env, failfast, start_at).expect("Deploy DB check failed")
            }
            "finalized_approvals" => FinalizedApprovalsDatabase::check_db(&env, failfast, start_at)
                .expect("Finalized Approvals DB check failed"),
            "proposers" => ProposerDatabase::check_db(&env, failfast, start_at)
                .expect("Proposer DB check failed"),
            "state_store" => StateStoreDatabase::check_db(&env, failfast, start_at)
                .expect("State Store DB check failed"),
            "transfer" => TransferDatabase::check_db(&env, failfast, start_at)
                .expect("Transfer DB check failed"),
            "transfer_hashes" => TransferHashesDatabase::check_db(&env, failfast, start_at)
                .expect("Transfer Hashes DB check failed"),
            _ => panic!("Database {} not found.", db_name),
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
