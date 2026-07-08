use std::sync::{Arc, Mutex};

pub(crate) struct ConnectionLimiter {
  active: Mutex<usize>,
  max:    usize,
}

impl ConnectionLimiter {
  pub(crate) fn new(max: usize) -> Self {
    Self {
      active: Mutex::new(0),
      max,
    }
  }

  pub(crate) fn acquire(self: &Arc<Self>) -> Option<ConnectionSlot> {
    let mut active = self.active.lock().expect("connection limit poisoned");
    if *active >= self.max {
      return None;
    }
    *active += 1;
    Some(ConnectionSlot {
      limiter: Arc::clone(self),
    })
  }
}

pub(crate) struct ConnectionSlot {
  limiter: Arc<ConnectionLimiter>,
}

impl Drop for ConnectionSlot {
  fn drop(&mut self) {
    let mut active = self
      .limiter
      .active
      .lock()
      .expect("connection limit poisoned");
    *active -= 1;
  }
}
