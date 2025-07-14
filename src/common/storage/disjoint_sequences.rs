use std::fmt::{self, Display, Formatter};

use casper_types::{
    AvailableBlockRange,
    bytesrepr::{self, FromBytes, ToBytes},
};
use itertools::Itertools;

/// Represents a continuous sequence of `u64`s.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) struct Sequence {
    /// The upper bound (inclusive) of the sequence.
    high: u64,
    /// The lower bound (inclusive) of the sequence.
    low: u64,
}

impl Sequence {
    /// Constructs a new sequence using the bounds of `a` and `b`.
    ///
    /// `low` and `high` will be automatically determined.
    pub(super) fn new(a: u64, b: u64) -> Self {
        let (low, high) = if a <= b { (a, b) } else { (b, a) };
        Sequence { low, high }
    }

    /// Constructs a new sequence containing only `value`.
    fn single(value: u64) -> Self {
        Sequence {
            high: value,
            low: value,
        }
    }

    /// Returns the inclusive high end of the sequence.
    pub(crate) fn high(&self) -> u64 {
        self.high
    }

    /// Returns the inclusive low end of the sequence.
    pub(crate) fn low(&self) -> u64 {
        self.low
    }
}

impl From<Sequence> for AvailableBlockRange {
    fn from(sequence: Sequence) -> Self {
        AvailableBlockRange::new(sequence.low(), sequence.high())
    }
}

/// Represents a collection of disjoint sequences of `u64`s.
///
/// The collection is kept ordered from high to low, and each entry represents a discrete portion of
/// the space from [0, u64::MAX] with a gap of at least 1 between each.
///
/// The collection is ordered this way to optimize insertion for the normal use case: adding
/// monotonically increasing values representing the latest block height.
///
/// As values are inserted, if two separate sequences become contiguous, they are merged into a
/// single sequence.
///
/// For example, if `sequences` contains `[9,9], [7,3]` and `8` is inserted, then `sequences` will
/// be reduced to `[9,3]`.
#[derive(Default, Debug)]
#[cfg_attr(test, derive(Clone))]
pub(super) struct DisjointSequences {
    sequences: Vec<Sequence>,
}

impl DisjointSequences {
    /// Constructs disjoint sequences from one initial sequence.
    ///
    /// Note: Use [`Default::default()`] to create an empty set of sequences.
    pub(super) fn new(initial_sequence: Sequence) -> Self {
        DisjointSequences {
            sequences: vec![initial_sequence],
        }
    }

    /// Reduces the sequence(s), keeping all entries below and including `max_value`.  If
    /// `max_value` is not already included in a sequence, it will not be added.
    ///
    /// If the current highest value is lower than `max_value`, or if there are no sequences, this
    /// has no effect.
    pub(super) fn truncate(&mut self, max_value: u64) {
        self.sequences.retain_mut(|sequence| {
            if sequence.high <= max_value {
                // Keep this sequence unchanged.
                return true;
            }

            if sequence.low > max_value {
                // Delete this entire sequence.
                return false;
            }

            // This sequence contains `max_value`, so keep the sequence, but reduce its high value.
            sequence.high = max_value;
            true
        })
    }
}

impl FromBytes for Sequence {
    #[inline]
    fn from_bytes(bytes: &[u8]) -> Result<(Self, &[u8]), bytesrepr::Error> {
        let (high, bytes) = u64::from_bytes(bytes)?;
        let (low, bytes) = u64::from_bytes(bytes)?;

        Ok((Sequence { high, low }, bytes))
    }
}

impl ToBytes for Sequence {
    #[inline]
    fn to_bytes(&self) -> Result<Vec<u8>, bytesrepr::Error> {
        let mut buf = Vec::new();
        self.write_bytes(&mut buf)?;
        Ok(buf)
    }

    #[inline]
    fn serialized_length(&self) -> usize {
        self.high.serialized_length() + self.low.serialized_length()
    }

    #[inline]
    fn write_bytes(&self, writer: &mut Vec<u8>) -> Result<(), bytesrepr::Error> {
        self.high.write_bytes(writer)?;
        self.low.write_bytes(writer)?;
        Ok(())
    }
}

impl FromBytes for DisjointSequences {
    fn from_bytes(bytes: &[u8]) -> Result<(Self, &[u8]), bytesrepr::Error> {
        Vec::<Sequence>::from_bytes(bytes)
            .map(|(sequences, remainder)| (DisjointSequences { sequences }, remainder))
    }

    #[inline]
    fn from_vec(bytes: Vec<u8>) -> Result<(Self, Vec<u8>), bytesrepr::Error> {
        Vec::<Sequence>::from_vec(bytes)
            .map(|(sequences, remainder)| (DisjointSequences { sequences }, remainder))
    }
}

impl ToBytes for DisjointSequences {
    #[inline]
    fn to_bytes(&self) -> Result<Vec<u8>, bytesrepr::Error> {
        self.sequences.to_bytes()
    }

    #[inline]
    fn serialized_length(&self) -> usize {
        self.sequences.serialized_length()
    }

    fn into_bytes(self) -> Result<Vec<u8>, bytesrepr::Error>
    where
        Self: Sized,
    {
        self.sequences.into_bytes()
    }

    fn write_bytes(&self, writer: &mut Vec<u8>) -> Result<(), bytesrepr::Error> {
        self.sequences.write_bytes(writer)
    }
}

/// This impl is provided to allow for efficient re-building of a `DisjointSequences` from a large,
/// randomly-ordered set of values.
impl From<Vec<u64>> for DisjointSequences {
    fn from(mut input: Vec<u64>) -> Self {
        input.sort_unstable();

        let sequences = input
            .drain(..)
            .peekable()
            .batching(|iter| match iter.next() {
                None => None,
                Some(low) => {
                    let mut sequence = Sequence::single(low);
                    while let Some(i) = iter.peek() {
                        if *i == sequence.high + 1 {
                            sequence.high = iter.next().unwrap();
                        }
                    }
                    Some(sequence)
                }
            })
            .collect();

        DisjointSequences { sequences }
    }
}

impl Display for DisjointSequences {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        let mut iter = self.sequences.iter().peekable();
        while let Some(sequence) = iter.next() {
            write!(formatter, "[{}, {}]", sequence.high, sequence.low)?;
            if iter.peek().is_some() {
                write!(formatter, ", ")?;
            }
        }
        Ok(())
    }
}
