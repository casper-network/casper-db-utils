use std::result::Result;

use log::warn;

// The tracker will log 20 times during its operation.
const STEPS: usize = 20;
// The tracker will log progress every in 5% completion intervals.
const PROGRESS_MULTIPLIER: u64 = 100 / STEPS as u64;
// Error message for initialization of a progress tracker with nothing
// to process.
const NULL_TOTAL_TO_PROCESS_ERROR: &str = "Cannot initialize total to process with 0";

/// Tracks and logs progress of an operation in a human readable form.
/// Whenever (1 / number of steps) of the total amount has been processed,
/// this structure calls the `log_progress` function, which takes the
/// percentage completed so far as a parameter.
pub struct ProgressTracker {
    /// Total amount there is to process.
    total_to_process: usize,
    /// Amount processed so far.
    processed: usize,
    /// Internal counter to keep track of the number of steps completed
    /// so far relative to the maximum amount of steps this operation
    /// will do, defined in `STEPS`.
    progress_factor: u64,
    /// Function which takes the completion rate as a percentage as
    /// input. It is called zero or more times as progress is being made
    /// using `ProgressTracker::advance_by`. The purpose of this function
    /// is to allow users to create custom log messages for their specific
    /// operation.
    log_progress: Box<dyn Fn(u64)>,
}

impl ProgressTracker {
    /// Create a new progress tracker by initializing it with a non-zero
    /// amount to be processed and a log function.
    pub fn new(
        total_to_process: usize,
        log_progress: Box<dyn Fn(u64)>,
    ) -> Result<Self, &'static str> {
        if total_to_process == 0 {
            Err(NULL_TOTAL_TO_PROCESS_ERROR)
        } else {
            Ok(Self {
                total_to_process,
                processed: 0,
                progress_factor: 1,
                log_progress,
            })
        }
    }

    /// Advance the progress tracker by a specific amount. If it passes
    /// a milestone ((1 / STEP) of the total amount to process),
    /// `log_progress` will be called with the current completion rate
    /// as input.
    pub fn advance_by(&mut self, step: usize) {
        self.processed += step;
        while self.processed * STEPS >= self.total_to_process * self.progress_factor as usize {
            (*self.log_progress)(self.progress_factor * PROGRESS_MULTIPLIER);
            self.progress_factor += 1;
        }
        if self.processed > self.total_to_process {
            warn!(
                "Exceeded total amount to process {} by {}",
                self.total_to_process,
                self.processed - self.total_to_process
            );
        }
    }
}
