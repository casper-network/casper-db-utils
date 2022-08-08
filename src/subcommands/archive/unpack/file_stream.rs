use std::{
    fs::OpenOptions,
    io::{Error as IoError, Read},
    path::Path,
    result::Result,
};

use log::{info, warn};

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
        let mut maybe_progress_tracker = None;
        match maybe_len {
            Some(len) => match ProgressTracker::new(
                len,
                Box::new(|completion| {
                    info!(
                        "Archive reading and decompressing {}% complete...",
                        completion
                    )
                }),
            ) {
                Ok(progress_tracker) => maybe_progress_tracker = Some(progress_tracker),
                Err(progress_tracker_error) => {
                    warn!(
                        "Couldn't initialize progress tracker: {}",
                        progress_tracker_error
                    )
                }
            },
            None => warn!("Unable to read file size, progress will not be logged."),
        }

        Self {
            reader,
            maybe_progress_tracker,
        }
    }
}

impl<R: Read> Read for FileStream<R> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, IoError> {
        let bytes_read = self.reader.read(buf)?;
        if let Some(progress_tracker) = self.maybe_progress_tracker.as_mut() {
            progress_tracker.advance_by(bytes_read);
        }
        Ok(bytes_read)
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
