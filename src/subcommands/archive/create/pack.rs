use std::{fs::OpenOptions, io as std_io, path::Path, result::Result, thread};

use log::info;

use super::Error;
use crate::subcommands::archive::{
    ring_buffer::BlockingRingBuffer, tar_utils::ArchiveStream, zstd_utils,
};

#[cfg(not(test))]
// 500 MiB.
const BUFFER_CAPACITY: usize = 500 * 1024 * 1024;
#[cfg(test)]
const BUFFER_CAPACITY: usize = 1_000;

pub fn create_archive<P1: AsRef<Path>, P2: AsRef<Path>>(
    db_dir_path: P1,
    dest: P2,
    overwrite: bool,
) -> Result<(), Error> {
    let ring_buffer = BlockingRingBuffer::new(BUFFER_CAPACITY);
    let (producer, mut consumer) = ring_buffer.split();

    let db_dir_path_copy = db_dir_path.as_ref().to_path_buf();
    let handle = thread::spawn(move || {
        let mut archive_stream =
            ArchiveStream::new(&db_dir_path_copy, producer).unwrap_or_else(|io_err| {
                panic!(
                    "Couldn't read files from {}: {}",
                    db_dir_path_copy.to_string_lossy(),
                    io_err
                )
            });
        archive_stream.pack().expect("Couldn't archive files");
    });

    let output_file = OpenOptions::new()
        .create_new(!overwrite)
        .write(true)
        .open(&dest)
        .map_err(Error::Destination)?;

    let mut encoder = zstd_utils::zstd_encode_stream(output_file)?;
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
