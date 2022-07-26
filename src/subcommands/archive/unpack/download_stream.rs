use std::{
    io::{Error as IoError, Read},
    path::Path,
    result::Result,
};

use futures::{io, AsyncRead, AsyncReadExt, TryStreamExt};
use log::info;
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
        Ok(Self {
            runtime,
            reader,
            maybe_progress_tracker: maybe_content_length.map(ProgressTracker::new),
        })
    }
}

impl Read for HttpStream {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, IoError> {
        let fut = async { self.reader.read(buf).await };
        let bytes_read = self.runtime.block_on(fut)?;
        if let Some(progress_tracker) = self.maybe_progress_tracker.as_mut() {
            progress_tracker.advance(bytes_read, |completion| {
                info!("Download {}% complete...", completion)
            });
        }
        Ok(bytes_read)
    }
}

impl Drop for HttpStream {
    fn drop(&mut self) {
        if let Some(progress_tracker) = self.maybe_progress_tracker.take() {
            progress_tracker.finish(|| info!("Download complete."));
        }
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
