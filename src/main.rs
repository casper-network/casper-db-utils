use clap::{App, Arg};

use casper_db_utils::{db, error::ToolError};

fn main() -> Result<(), ToolError> {
    let matches = App::new("lmdb-util")
        .arg(
            Arg::new("db-path")
                .required(true)
                .short('d')
                .long("db-path")
                .takes_value(true)
                .value_name("DB_PATH")
                .help("Path to the storage.lmdb file"),
        )
        .arg(
            Arg::new("block-height")
                .required(true)
                .short('b')
                .long("block-height")
                .takes_value(true)
                .value_name("BLOCK_HEIGHT")
                .help("blocks above this height will be deleted"),
        )
        .get_matches();

    let path = matches.value_of("db-path").unwrap();
    let block_height = matches
        .value_of("block-height")
        .unwrap()
        .parse::<u64>()
        .unwrap();

    let (deleted_headers, deleted_bodies, deleted_metas) = db::run(path.into(), block_height)?;

    println!(
        "Deleted: {} block headers, {} block bodies and {} block metas",
        deleted_headers, deleted_bodies, deleted_metas
    );

    Ok(())
}
