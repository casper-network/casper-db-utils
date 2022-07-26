use std::{
    fs::OpenOptions,
    io::{Error as IoError, Read},
    path::Path,
    result::Result,
};

use log::info;

use super::Error;
use crate::{
    common::progress::ProgressTracker,
    subcommands::archive::{tar_utils, zstd_utils},
};

struct FileStream<R> {
    reader: R,
    maybe_progress_tracker: Option<ProgressTracker>,
}

impl<R: Read> FileStream<R> {
    fn new(reader: R, maybe_len: Option<usize>) -> Self {
        Self {
            reader,
            maybe_progress_tracker: maybe_len.map(ProgressTracker::new),
        }
    }
}

impl<R: Read> Read for FileStream<R> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, IoError> {
        let bytes_read = self.reader.read(buf)?;
        if let Some(progress_tracker) = self.maybe_progress_tracker.as_mut() {
            progress_tracker.advance(bytes_read, |completion| {
                info!("Archive reading {}% complete...", completion)
            });
        }
        Ok(bytes_read)
    }
}

impl<R> Drop for FileStream<R> {
    fn drop(&mut self) {
        if let Some(progress_tracker) = self.maybe_progress_tracker.take() {
            progress_tracker.finish(|| info!("Decompression complete."));
        }
    }
}

pub fn file_stream_and_unpack_archive<P1: AsRef<Path>, P2: AsRef<Path>>(
    path: P1,
    dest: P2,
) -> Result<(), Error> {
    let input_file = OpenOptions::new()
        .read(true)
        .open(path)
        .map_err(Error::Source)?;
    let file_len: Option<usize> = input_file
        .metadata()
        .ok()
        .and_then(|metadata| metadata.len().try_into().ok());
    let file_stream = FileStream::new(input_file, file_len);
    let decoder = zstd_utils::zstd_decode_stream(file_stream)?;
    let mut unpacker = tar_utils::unarchive_stream(decoder);
    unpacker.unpack(dest).map_err(Error::Streaming)?;
    Ok(())
}
