pub mod check;
pub mod execution_results_summary;
pub mod extract_slice;
pub mod latest_block_summary;
pub mod purge_signatures;
pub mod remove_block;
pub mod trie_compact;
pub mod unsparse;

use thiserror::Error as ThisError;

use check::Error as CheckError;
use execution_results_summary::Error as ExecutionResultsSummaryError;
use extract_slice::Error as ExtractSliceError;
use latest_block_summary::Error as LatestBlockSummaryError;
use purge_signatures::Error as PurgeSignaturesError;
use remove_block::Error as RemoveBlockError;
use unsparse::Error as UnsparseError;

#[derive(ThisError, Debug)]
pub enum Error {
    #[error("Check command failed: {0}")]
    Check(#[from] CheckError),
    #[error("Execution results summary command failed: {0}")]
    ExecutionResultsSummary(#[from] ExecutionResultsSummaryError),
    #[error("Extract slice command failed: {0}")]
    ExtractSlice(#[from] ExtractSliceError),
    #[error("Latest block summary command failed: {0}")]
    LatestBlockSummary(#[from] LatestBlockSummaryError),
    #[error("Purge signatures failed: {0}")]
    PurgeSignatures(#[from] PurgeSignaturesError),
    #[error("Remove block failed: {0}")]
    RemoveBlock(#[from] RemoveBlockError),
    #[error("Unsparse failed: {0}")]
    Unsparse(#[from] UnsparseError),
    #[error("Trie compact failed: {0}")]
    TrieCompact(#[from] trie_compact::Error),
}
