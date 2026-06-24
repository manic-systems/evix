use std::sync::{Arc, atomic::AtomicBool};

use anyhow::Result;

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
) -> Result<(EvalGraph, Vec<EvalError>)>
where
  F: FnMut(Event) -> Result<()>,
{
  let mut accumulator = EvalAccumulator::default();

  crate::async_master::run(config, Arc::clone(&cancel), |event| {
    accumulator.record(&event);
    let result = on_event(event);
    async move { result }
  })
  .await?;

  Ok((accumulator.graph, accumulator.errors))
}
