use std::{
    io::{Error as IoError, Read},
    path::Path,
    result::Result,
};

use futures::{io, AsyncRead, AsyncReadExt, TryStreamExt};
use log::{info, warn};
use tokio::runtime::{Builder as TokioRuntimeBuilder, Runtime};

use super::Error;
use crate::{
    common::progress::ProgressTracker,
    subcommands::archive::{tar_utils, zstd_utils},
};

struct HttpStream {
    runtime: Runtime,
    reader: Box<dyn AsyncRead + Unpin>,
    maybe_progress_tracker: Option<ProgressTracker>,
}

impl HttpStream {
    fn new(runtime: Runtime, url: &str) -> Result<Self, Error> {
        let response_future = async {
            let response_fut = reqwest::get(url).await;
            match response_fut {
                Ok(response) => {
                    let maybe_len = response.content_length().and_then(|len| {
                        info!("Download size: {} bytes.", len);
                        len.try_into().ok()
                    });
                    Ok((
                        response.bytes_stream().map_err(|reqwest_err| {
                            io::Error::new(io::ErrorKind::Other, reqwest_err)
                        }),
                        maybe_len,
                    ))
                }
                Err(request_err) => Err(Error::Request(request_err)),
            }
        };
        let (http_stream, maybe_content_length) = runtime.block_on(response_future)?;
        let http_stream = http_stream.into_async_read();
        let reader = Box::new(http_stream) as Box<dyn AsyncRead + Unpin>;
        let mut maybe_progress_tracker = None;
        match maybe_content_length {
            Some(len) => match ProgressTracker::new(
                len,
                Box::new(|completion| info!("Download {}% complete...", completion)),
            ) {
                Ok(progress_tracker) => maybe_progress_tracker = Some(progress_tracker),
                Err(progress_tracker_error) => {
                    warn!(
                        "Couldn't initialize progress tracker: {}",
                        progress_tracker_error
                    )
                }
            },
            None => warn!("No stream length provided, progress will not be logged."),
        }

        Ok(Self {
            runtime,
            reader,
            maybe_progress_tracker,
        })
    }
}

impl Read for HttpStream {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, IoError> {
        let fut = async { self.reader.read(buf).await };
        let bytes_read = self.runtime.block_on(fut)?;
        if let Some(progress_tracker) = self.maybe_progress_tracker.as_mut() {
            progress_tracker.advance_by(bytes_read);
        }
        Ok(bytes_read)
    }
}

pub fn download_and_unpack_archive<P: AsRef<Path>>(url: &str, dest: P) -> Result<(), Error> {
    let runtime = TokioRuntimeBuilder::new_current_thread()
        .enable_time()
        .enable_io()
        .build()
        .map_err(Error::Runtime)?;
    let http_stream = HttpStream::new(runtime, url)?;
    let decoder = zstd_utils::zstd_decode_stream(http_stream)?;
    let mut unpacker = tar_utils::unarchive_stream(decoder);
    unpacker.unpack(&dest).map_err(Error::Streaming)?;
    Ok(())
}
