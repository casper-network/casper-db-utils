use std::{
    fs::OpenOptions,
    io::{self as std_io, Read},
    path::PathBuf,
    result::Result,
};

use futures::{io, AsyncRead, AsyncReadExt, TryStreamExt};
use log::info;
use tokio::runtime::{Builder as TokioRuntimeBuilder, Runtime};

use super::zstd_decode;
use super::Error;

pub struct StreamPipe {
    runtime: Runtime,
    reader: Box<dyn AsyncRead + Unpin>,
    pub stream_length: Option<usize>,
    pub total_bytes_read: usize,
    pub progress: u64,
}

impl StreamPipe {
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

impl Read for StreamPipe {
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

pub fn download_archive(
    url: &str,
    dest: PathBuf,
    zstd_decode: bool,
    log_distance: Option<u32>,
) -> Result<(), Error> {
    let mut output_file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(dest)
        .map_err(Error::Destination)?;
    let runtime = TokioRuntimeBuilder::new_current_thread()
        .enable_time()
        .enable_io()
        .build()
        .map_err(Error::Runtime)?;
    let mut stream_pipe = StreamPipe::new(runtime, url)?;
    let decoded_bytes = if zstd_decode {
        let mut decoder = zstd_decode::zstd_decode_stream(stream_pipe, log_distance)?;
        std_io::copy(&mut decoder, &mut output_file).map_err(Error::Streaming)?
    } else {
        std_io::copy(&mut stream_pipe, &mut output_file).map_err(Error::Streaming)?
    };
    info!("Download complete.");
    if zstd_decode {
        info!("Decoded {} bytes.", decoded_bytes);
    }
    Ok(())
}
