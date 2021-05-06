use std::time::Duration;

use futures_retry::{ErrorHandler, RetryPolicy};

use super::exception::Exception;

pub(crate) struct FixedCountWithDelayStrategy {
    max_attempts: usize,
    delay: Duration,
}

impl FixedCountWithDelayStrategy {
    pub(crate) fn new(max_attempts: usize, delay: Duration) -> Self {
        Self {
            max_attempts,
            delay,
        }
    }
}

impl ErrorHandler<Exception> for FixedCountWithDelayStrategy {
    type OutError = Exception;

    fn handle(&mut self, attempt: usize, e: Exception) -> RetryPolicy<Exception> {
        if attempt >= self.max_attempts {
            return RetryPolicy::ForwardError(e);
        }

        match e {
            Exception::Retryable(_) => RetryPolicy::WaitRetry(self.delay),
            Exception::NonRetryable(_) => RetryPolicy::ForwardError(e),
        }
    }
}
