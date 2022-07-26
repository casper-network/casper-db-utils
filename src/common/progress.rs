pub struct ProgressTracker {
    total_to_process: usize,
    processed: usize,
    progress_factor: u64,
}

impl ProgressTracker {
    pub fn new(total_to_process: usize) -> Self {
        Self {
            total_to_process,
            processed: 0,
            progress_factor: 1,
        }
    }

    pub fn advance<F: Fn(u64)>(&mut self, step: usize, log_progress: F) {
        self.processed += step;
        while self.processed > (self.total_to_process * self.progress_factor as usize) / 20 {
            log_progress(self.progress_factor * 5);
            self.progress_factor += 1;
        }
    }

    pub fn finish<F: Fn()>(self, log_completion: F) {
        log_completion()
    }
}
