use std::{io::Read, path::Path, result::Result};

use futures::{io, AsyncRead, AsyncReadExt, TryStreamExt};
use log::info;
use tokio::runtime::{Builder as TokioRuntimeBuilder, Runtime};

use super::Error;
use crate::subcommands::archive::{tar_utils, zstd_utils};

pub struct HttpStream {
    runtime: Runtime,
    reader: Box<dyn AsyncRead + Unpin>,
    pub stream_length: Option<usize>,
    pub total_bytes_read: usize,
    pub progress: u64,
}

impl HttpStream {
    fn new(runtime: Runtime, url: &str) -> Result<Self, Error> {
        let response_future = async {
            let response_fut = reqwest::get(url).await;
            match response_fut {
                Ok(response) => {
                    let maybe_len = response.content_length().map(|len| {
                        info!("Download size: {} bytes.", len);
                        // Highly unlikely scenario where we can't convert `u64` to `usize`.
                        // This would mean we're running on a 32-bit or older system and the
                        // content length cannot be represented in that system's pointer size.
                        len.try_into()
                            .expect("Couldn't convert content length from u64 to usize")
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
            stream_length: maybe_content_length,
            total_bytes_read: 0,
            progress: 1,
        })
    }
}

impl Read for HttpStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let fut = async { self.reader.read(buf).await };
        let bytes_read = self.runtime.block_on(fut)?;
        self.total_bytes_read += bytes_read;
        if let Some(stream_len) = self.stream_length {
            while self.total_bytes_read > (stream_len * self.progress as usize) / 20 {
                info!("Download {}% complete...", self.progress * 5);
                self.progress += 1;
            }
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
    info!("Download complete.");
    Ok(())
}
