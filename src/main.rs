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
            Arg::new("db-path")
                .required(true)
                .short('d')
                .long("db-path")
                .takes_value(true)
                .value_name("DB_PATH")
                .help("Path to the storage.lmdb file"),
        )
        .get_matches();

    let path = matches.value_of("db-path").unwrap();
    let env = db_env(path.into()).expect("failed to initialize DB environment");
    BlockBodyDatabase::check_db(&env).expect("DB check failed");
    BlockBodyMerkleDatabase::check_db(&env).expect("DB check failed");
    BlockHeaderDatabase::check_db(&env).expect("DB check failed");
    BlockMetadataDatabase::check_db(&env).expect("DB check failed");
    DeployHashesDatabase::check_db(&env).expect("DB check failed");
    DeployMetadataDatabase::check_db(&env).expect("DB check failed");
    DeployDatabase::check_db(&env).expect("DB check failed");
    FinalizedApprovalsDatabase::check_db(&env).expect("DB check failed");
    ProposerDatabase::check_db(&env).expect("DB check failed");
    StateStoreDatabase::check_db(&env).expect("DB check failed");
    TransferDatabase::check_db(&env).expect("DB check failed");
    TransferHashesDatabase::check_db(&env).expect("DB check failed");
}
