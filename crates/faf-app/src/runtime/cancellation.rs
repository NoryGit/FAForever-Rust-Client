//! Service-owned cancellation. A later run never clears an older run's token.

#[derive(Default)]
pub struct CancellationSlot(std::sync::Mutex<Option<tokio_util::sync::CancellationToken>>);

impl CancellationSlot {
    pub fn begin(&self) -> tokio_util::sync::CancellationToken {
        let token = tokio_util::sync::CancellationToken::new();
        if let Some(previous) = self.0.lock().unwrap().replace(token.clone()) {
            previous.cancel();
        }
        token
    }

    pub fn cancel(&self) {
        if let Some(token) = self.0.lock().unwrap().as_ref() {
            token.cancel();
        }
    }

    pub fn is_cancelled(&self) -> bool {
        self.0
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|token| token.is_cancelled())
    }
}
