mod compact;
mod helpers;
// All code in the `utils` mod was copied from `casper-node` because
// it wasn't available in the public interface.
// TODO: make them available in order to import them directly.
mod utils;

pub use compact::{trie_compact, DestinationOptions, Error, DEFAULT_MAX_DB_SIZE};
