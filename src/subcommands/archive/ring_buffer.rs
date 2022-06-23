use std::{
    io::{ErrorKind, Read, Write},
    sync::{Arc, RwLock},
};

use ringbuf::{Consumer, Producer, RingBuffer};

pub struct BlockingRingBuffer {
    inner: RingBuffer<u8>,
}

impl BlockingRingBuffer {
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: RingBuffer::new(capacity),
        }
    }

    pub fn split(self) -> (BlockingProducer, BlockingConsumer) {
        let (producer, consumer) = self.inner.split();
        let eof = Arc::new(RwLock::new(false));
        (
            BlockingProducer::new(producer, eof.clone()),
            BlockingConsumer::new(consumer, eof),
        )
    }
}

pub struct BlockingConsumer {
    pub inner: Consumer<u8>,
    eof: Arc<RwLock<bool>>,
}

impl BlockingConsumer {
    pub fn new(inner: Consumer<u8>, eof: Arc<RwLock<bool>>) -> Self {
        Self { inner, eof }
    }
}

impl Read for BlockingConsumer {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self.inner.read(buf) {
            Ok(n) => Ok(n),
            Err(io_err) if io_err.kind() == ErrorKind::WouldBlock => loop {
                if *self.eof.read().expect("Poisoned lock") {
                    return Ok(0);
                }

                std::thread::sleep(std::time::Duration::from_micros(10));
                if let Ok(n) = self.inner.read(buf) {
                    return Ok(n);
                }
            },
            Err(err) => Err(err),
        }
    }
}

pub struct BlockingProducer {
    pub inner: Producer<u8>,
    eof: Arc<RwLock<bool>>,
}

impl BlockingProducer {
    pub fn new(inner: Producer<u8>, eof: Arc<RwLock<bool>>) -> Self {
        Self { inner, eof }
    }
}

impl Write for BlockingProducer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self.inner.write(buf) {
            Ok(n) => Ok(n),
            Err(io_err) if io_err.kind() == ErrorKind::WouldBlock => loop {
                std::thread::sleep(std::time::Duration::from_millis(1));
                if let Ok(n) = self.inner.write(buf) {
                    return Ok(n);
                }
            },
            Err(err) => Err(err),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Drop for BlockingProducer {
    fn drop(&mut self) {
        *self.eof.write().expect("Poisoned lock") = true;
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io::{self, Error as IoError},
        thread::{self, JoinHandle},
    };

    use rand::RngCore;

    use super::BlockingRingBuffer;

    const BUFFER_CAPACITY: usize = 10;

    #[test]
    fn ring_buffer_roundtrip() {
        let mut rng = rand::thread_rng();
        let mut original_payload = [0u8; 1000];
        rng.fill_bytes(&mut original_payload);
        let payload = original_payload.clone().to_vec();
        let mut message: Vec<u8> = vec![];

        let ring_buffer = BlockingRingBuffer::new(BUFFER_CAPACITY);
        let (mut producer, mut consumer) = ring_buffer.split();

        let producer_handle =
            thread::spawn(move || io::copy(&mut payload.as_slice(), &mut producer));

        let consumer_handle: JoinHandle<Result<Vec<u8>, IoError>> = thread::spawn(move || {
            io::copy(&mut consumer, &mut message)?;
            Ok(message)
        });

        assert!(producer_handle.join().is_ok());
        let message = consumer_handle
            .join()
            .expect("Thread copying from consumer panicked")
            .expect("Copying from consumer into message failed");
        assert_eq!(original_payload, message.as_slice());
    }
}
