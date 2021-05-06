#[allow(dead_code)]
pub(crate) enum Exception {
    Retryable(String),
    NonRetryable(String),
}
