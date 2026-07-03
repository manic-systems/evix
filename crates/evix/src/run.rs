use std::{
  future::Future,
  sync::{Arc, atomic::AtomicBool},
};

use anyhow::Result;

pub(crate) use crate::async_master::RunOutcome;
use crate::{
  Config,
  EvalError,
  Event,
  state::{EvalAccumulator, EvalGraph},
};

pub async fn evaluate<F>(
  config: Config,
  cancel: Arc<AtomicBool>,
  mut on_event: F,
) -> Result<(EvalGraph, Vec<EvalError>, RunOutcome)>
where
  F: FnMut(Event) -> Result<()>,
{
  let mut accumulator = EvalAccumulator::default();

  let outcome =
    crate::async_master::run(config, Arc::clone(&cancel), |event| {
      accumulator.record(&event);
      let result = on_event(event);
      async move { result }
    })
    .await?;

  Ok((accumulator.graph, accumulator.errors, outcome))
}

pub async fn evaluate_async<F, Fut>(
  config: Config,
  cancel: Arc<AtomicBool>,
  mut on_event: F,
) -> Result<(EvalGraph, Vec<EvalError>, RunOutcome)>
where
  F: FnMut(Event) -> Fut,
  Fut: Future<Output = Result<()>>,
{
  let mut accumulator = EvalAccumulator::default();

  let outcome =
    crate::async_master::run(config, Arc::clone(&cancel), |event| {
      accumulator.record(&event);
      on_event(event)
    })
    .await?;

  Ok((accumulator.graph, accumulator.errors, outcome))
}
