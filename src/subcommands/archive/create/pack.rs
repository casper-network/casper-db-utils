use std::{
    fs::{self, OpenOptions},
    io::{self as std_io, BufReader},
    path::{Path, PathBuf},
    result::Result,
    thread,
};

use log::{info, warn};

use super::Error;
use crate::subcommands::archive::{
    ring_buffer::BlockingRingBuffer,
    tar_utils::{self, ArchiveStream},
    zstd_utils,
};

#[cfg(not(test))]
// 500 MiB.
const BUFFER_CAPACITY: usize = 500 * 1024 * 1024;
#[cfg(test)]
const BUFFER_CAPACITY: usize = 1_000;

#[allow(unused)]
pub fn create_archive<P1: AsRef<Path>, P2: AsRef<Path>>(
    db_path: P1,
    dest: P2,
    require_checksums: bool,
) -> Result<(), Error> {
    let temp_tarball_path: PathBuf = "/tmp/temp_casper_db.tar".into();
    info!(
        "Packing contents at {} to tarball.",
        db_path.as_ref().as_os_str().to_string_lossy()
    );
    tar_utils::archive(db_path, &temp_tarball_path).map_err(Error::Tar)?;
    info!(
        "Successfully created temporary tarball at {}",
        temp_tarball_path.as_os_str().to_string_lossy()
    );

    let mut temp_tarball_file = BufReader::new(
        OpenOptions::new()
            .read(true)
            .open(&temp_tarball_path)
            .map_err(Error::Destination)?,
    );
    let output_file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&dest)
        .map_err(Error::Destination)?;

    let mut encoder = zstd_utils::zstd_encode_stream(output_file, require_checksums)?;
    let _ = std_io::copy(&mut temp_tarball_file, &mut encoder).map_err(Error::Streaming)?;
    encoder.finish().map_err(Error::Streaming)?;
    info!(
        "Finished encoding tarball with zstd, compressed archive at {}",
        dest.as_ref().as_os_str().to_string_lossy()
    );
    if let Err(io_err) = fs::remove_file(&temp_tarball_path) {
        warn!(
            "Couldn't remove tarball at {} after compression: {}",
            temp_tarball_path.as_os_str().to_string_lossy(),
            io_err
        );
    }
    Ok(())
}

pub fn create_archive_streamed<P1: AsRef<Path>, P2: AsRef<Path>>(
    db_dir_path: P1,
    dest: P2,
    require_checksums: bool,
) -> Result<(), Error> {
    let ring_buffer = BlockingRingBuffer::new(BUFFER_CAPACITY);
    let (producer, mut consumer) = ring_buffer.split();

    let db_dir_path_copy = db_dir_path.as_ref().to_path_buf();
    let handle = thread::spawn(move || {
        let mut archive_stream =
            ArchiveStream::new(&db_dir_path_copy, producer).unwrap_or_else(|_| {
                panic!(
                    "Couldn't read files from {}",
                    db_dir_path_copy.to_string_lossy()
                )
            });
        archive_stream.pack().expect("Couldn't archive files");
    });

    let output_file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&dest)
        .map_err(Error::Destination)?;

    let mut encoder = zstd_utils::zstd_encode_stream(output_file, require_checksums)?;
    let _ = std_io::copy(&mut consumer, &mut encoder).map_err(Error::Streaming)?;
    encoder.finish().map_err(Error::Streaming)?;

    handle
        .join()
        .map(|_| {
            info!(
                "Finished encoding tarball with zstd, compressed archive at {}",
                dest.as_ref().display()
            )
        })
        .map_err(|_| Error::ArchiveStream)
}
