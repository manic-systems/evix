use std::{
  future::Future,
  sync::{Arc, atomic::AtomicBool},
};

use anyhow::Result;
use futures_util::future;
use tokio::sync::Mutex;

use crate::{
  Config,
  EvalError,
  Event,
  Remote,
  state::{EvalAccumulator, EvalGraph},
};

#[derive(Clone, Copy)]
enum EventSource {
  Local,
  Remote,
}

pub async fn evaluate<F, Fut>(
  config: Config,
  cancel: Arc<AtomicBool>,
  on_event: F,
) -> Result<(EvalGraph, Vec<EvalError>)>
where
  F: FnMut(Event) -> Fut + Send + 'static,
  Fut: Future<Output = Result<()>> + Send + 'static,
{
  let accumulator = Arc::new(Mutex::new(EvalAccumulator::default()));
  let on_event = Arc::new(Mutex::new(on_event));
  let remotes = config.remotes.clone();

  let local = crate::async_master::run(config.clone(), Arc::clone(&cancel), {
    let accumulator = Arc::clone(&accumulator);
    let on_event = Arc::clone(&on_event);
    let remotes = remotes.clone();
    move |event| {
      let accumulator = Arc::clone(&accumulator);
      let on_event = Arc::clone(&on_event);
      let remotes = remotes.clone();
      async move {
        record_event(EventSource::Local, &remotes, &accumulator, &event).await;
        let mut sink = on_event.lock().await;
        (*sink)(event).await
      }
    }
  });

  let remote = crate::remote::run(config, cancel, {
    let accumulator = Arc::clone(&accumulator);
    move |event| {
      let accumulator = Arc::clone(&accumulator);
      let on_event = Arc::clone(&on_event);
      async move {
        record_event(EventSource::Remote, &[], &accumulator, &event).await;
        let mut sink = on_event.lock().await;
        (*sink)(event).await
      }
    }
  });

  future::try_join(local, remote).await?;

  let mut accumulator = accumulator.lock().await;
  Ok((
    std::mem::take(&mut accumulator.graph),
    std::mem::take(&mut accumulator.errors),
  ))
}

async fn record_event(
  source: EventSource,
  remotes: &[Remote],
  accumulator: &Mutex<EvalAccumulator>,
  event: &Event,
) {
  if should_record_event(source, remotes, event) {
    accumulator.lock().await.record(event);
  }
}

fn should_record_event(
  source: EventSource,
  remotes: &[Remote],
  event: &Event,
) -> bool {
  match source {
    EventSource::Remote => true,
    EventSource::Local => {
      let Event::Derivation(drv) = event else {
        return true;
      };
      !remotes.iter().any(|remote| {
        remote.systems.is_empty()
          || remote.systems.iter().any(|system| system == &drv.system)
      })
    },
  }
}
