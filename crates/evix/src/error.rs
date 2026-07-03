use std::{error, fmt};

/// Error type returned by Evix's public library APIs.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
  /// `Session::stream` was requested after the single-use stream had already
  /// started or completed.
  SessionStreamConsumed,
  /// A warm-graph operation was requested before initial evaluation completed.
  InitialEvaluationIncomplete { operation: &'static str },
  /// A session operation requires completion, but evaluation is still running.
  SessionStillEvaluating,
  /// Initial evaluation failed and the stored error is being reported again.
  EvaluationFailed { message: String },
  /// A background task could not be spawned because no Tokio runtime was
  /// active.
  RuntimeUnavailable { message: String },
  /// Evaluation was cancelled before it completed.
  Cancelled,
  /// An internal evaluator, worker, I/O, serialization, or protocol error.
  Internal { message: String },
}

/// Result type returned by Evix's public library APIs.
pub type Result<T> = std::result::Result<T, Error>;

impl Error {
  pub(crate) fn internal(err: anyhow::Error) -> Self {
    Self::Internal {
      message: err.to_string(),
    }
  }
}

impl fmt::Display for Error {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      Self::SessionStreamConsumed => {
        write!(f, "session stream has already been consumed")
      },
      Self::InitialEvaluationIncomplete { operation } => {
        write!(
          f,
          "Session::{operation} requires a completed initial evaluation"
        )
      },
      Self::SessionStillEvaluating => write!(f, "session is still evaluating"),
      Self::Cancelled => write!(f, "evaluation was cancelled"),
      Self::EvaluationFailed { message }
      | Self::RuntimeUnavailable { message }
      | Self::Internal { message } => f.write_str(message),
    }
  }
}

impl error::Error for Error {}

impl From<anyhow::Error> for Error {
  fn from(err: anyhow::Error) -> Self {
    Self::internal(err)
  }
}
