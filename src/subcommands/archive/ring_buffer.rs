use std::{
    io::{ErrorKind, Read, Result as IoResult, Write},
    sync::{Arc, Condvar, Mutex},
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
        let condvar = Arc::new(Condvar::new());
        let done = Arc::new(Mutex::new(false));
        (
            BlockingProducer::new(producer, condvar.clone(), done.clone()),
            BlockingConsumer::new(consumer, condvar, done),
        )
    }
}

pub struct BlockingConsumer {
    inner: Consumer<u8>,
    condvar: Arc<Condvar>,
    done: Arc<Mutex<bool>>,
}

impl BlockingConsumer {
    fn new(inner: Consumer<u8>, condvar: Arc<Condvar>, done: Arc<Mutex<bool>>) -> Self {
        Self {
            inner,
            condvar,
            done,
        }
    }
}

impl Read for BlockingConsumer {
    fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
        loop {
            match self.inner.read(buf) {
                Ok(bytes_read) => {
                    let _done = self.done.lock().expect("poisoned lock");
                    self.condvar.notify_one();
                    return Ok(bytes_read);
                }
                Err(io_err) if io_err.kind() == ErrorKind::WouldBlock => {
                    let done = self
                        .condvar
                        .wait_while(self.done.lock().expect("poisoned lock"), |&mut done| {
                            !done && self.inner.is_empty()
                        })
                        .expect("poisoned lock while waiting");
                    if *done && self.inner.is_empty() {
                        return Ok(0);
                    }
                }
                Err(err) => return Err(err),
            }
        }
    }
}

impl Drop for BlockingConsumer {
    fn drop(&mut self) {
        *self.done.lock().expect("poisoned lock") = true;
        self.condvar.notify_one();
    }
}

pub struct BlockingProducer {
    inner: Producer<u8>,
    condvar: Arc<Condvar>,
    done: Arc<Mutex<bool>>,
}

impl BlockingProducer {
    fn new(inner: Producer<u8>, condvar: Arc<Condvar>, done: Arc<Mutex<bool>>) -> Self {
        Self {
            inner,
            condvar,
            done,
        }
    }
}

impl Write for BlockingProducer {
    fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
        loop {
            match self.inner.write(buf) {
                Ok(bytes_written) => {
                    let _done = self.done.lock().expect("poisoned lock");
                    self.condvar.notify_one();
                    return Ok(bytes_written);
                }
                Err(io_err) if io_err.kind() == ErrorKind::WouldBlock => {
                    let done = self
                        .condvar
                        .wait_while(self.done.lock().expect("poisoned lock"), |&mut done| {
                            !done && self.inner.is_full()
                        })
                        .expect("poisoned lock while waiting");
                    if *done && self.inner.is_full() {
                        return Ok(0);
                    }
                }
                Err(err) => return Err(err),
            }
        }
    }

    fn flush(&mut self) -> IoResult<()> {
        Ok(())
    }
}

impl Drop for BlockingProducer {
    fn drop(&mut self) {
        *self.done.lock().expect("poisoned lock") = true;
        self.condvar.notify_one();
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
