use std::{collections::BTreeMap, path::PathBuf};

use anyhow::{Context as _, Result, bail};
use capnp::message::{Builder, HeapAllocator, ReaderOptions};
use futures_util::{AsyncRead, AsyncWrite, AsyncWriteExt as _};

use crate::{
  AutoArg,
  Derivation,
  EvalError,
  Event,
  Input,
  worker_capnp,
  worker_config::{DEFAULT_ITEM_TIMEOUT_SECONDS, WorkerConfig},
  worker_process::WorkerStatus,
};

pub(crate) const WORKER_PROTOCOL_VERSION: u32 = 1;
const REMOTE_TRAVERSAL_LIMIT_WORDS: usize = 8 * 1024 * 1024;
const LOCAL_TRAVERSAL_LIMIT_WORDS: usize = 64 * 1024 * 1024;
const NESTING_LIMIT: i32 = 64;

#[derive(Debug)]
pub(crate) enum ClientMessage {
  Setup {
    config:             WorkerConfig,
    token:              Option<String>,
    expected_store_dir: Option<String>,
  },
  Work(Vec<String>),
  Shutdown,
}

#[derive(Debug)]
pub(crate) enum ServerMessage {
  Ready,
  Event(Box<Event>),
  Status(WorkerStatus),
  Error(String),
}

pub(crate) async fn write_client<W>(
  writer: &mut W,
  message: &ClientMessage,
) -> Result<()>
where
  W: AsyncWrite + Unpin,
{
  let mut builder = Builder::new_default();
  {
    let mut root = builder.init_root::<worker_capnp::client_message::Builder>();
    match message {
      ClientMessage::Setup {
        config,
        token,
        expected_store_dir,
      } => {
        let mut setup = root.reborrow().init_setup();
        set_worker_config(setup.reborrow().init_config(), config)?;
        set_text_opt(setup.reborrow().init_token(), token.as_deref());
        set_text_opt(
          setup.reborrow().init_expected_store_dir(),
          expected_store_dir.as_deref(),
        );
        setup.set_protocol_version(WORKER_PROTOCOL_VERSION);
      },
      ClientMessage::Work(path) => {
        set_text_list(
          root
            .reborrow()
            .init_work()
            .init_attr_path(list_len(path.len(), "work attr path")?),
          path,
        )?;
      },
      ClientMessage::Shutdown => root.set_shutdown(()),
    }
  }
  capnp_futures::serialize::write_message(&mut *writer, &builder).await?;
  writer.flush().await?;
  Ok(())
}

pub(crate) async fn read_client<R>(reader: &mut R) -> Result<ClientMessage>
where
  R: AsyncRead + Unpin,
{
  read_client_with_options(reader, remote_reader_options()).await
}

pub(crate) async fn read_local_client<R>(
  reader: &mut R,
) -> Result<ClientMessage>
where
  R: AsyncRead + Unpin,
{
  read_client_with_options(reader, local_reader_options()).await
}

async fn read_client_with_options<R>(
  reader: &mut R,
  options: ReaderOptions,
) -> Result<ClientMessage>
where
  R: AsyncRead + Unpin,
{
  let message = capnp_futures::serialize::read_message(reader, options).await?;
  let root = message.get_root::<worker_capnp::client_message::Reader>()?;
  match root.which()? {
    worker_capnp::client_message::Which::Setup(setup) => {
      let setup = setup?;
      validate_worker_protocol(setup.get_protocol_version())?;
      Ok(ClientMessage::Setup {
        config:             read_worker_config(setup.get_config()?)?,
        token:              read_text_opt(setup.get_token()?)?,
        expected_store_dir: read_text_opt(setup.get_expected_store_dir()?)?,
      })
    },
    worker_capnp::client_message::Which::Work(work) => {
      Ok(ClientMessage::Work(read_text_list(work?.get_attr_path()?)?))
    },
    worker_capnp::client_message::Which::Shutdown(()) => {
      Ok(ClientMessage::Shutdown)
    },
  }
}

fn validate_worker_protocol(actual: u32) -> Result<()> {
  if actual == WORKER_PROTOCOL_VERSION {
    Ok(())
  } else {
    bail!(
      "unsupported worker protocol version {}; expected {}",
      actual,
      WORKER_PROTOCOL_VERSION
    )
  }
}

pub(crate) async fn write_server<W>(
  writer: &mut W,
  message: &ServerMessage,
) -> Result<()>
where
  W: AsyncWrite + Unpin,
{
  let mut builder = Builder::new(HeapAllocator::new());
  {
    let mut root = builder.init_root::<worker_capnp::server_message::Builder>();
    match message {
      ServerMessage::Ready => root.set_ready(()),
      ServerMessage::Event(event) => {
        set_event(root.reborrow().init_event(), event)?;
      },
      ServerMessage::Status(status) => {
        root.set_status(match status {
          WorkerStatus::Ready => worker_capnp::WorkerStatus::Ready,
          WorkerStatus::Restart => worker_capnp::WorkerStatus::Restart,
        })
      },
      ServerMessage::Error(error) => root.set_error(error),
    }
  }
  capnp_futures::serialize::write_message(&mut *writer, &builder).await?;
  writer.flush().await?;
  Ok(())
}

pub(crate) async fn read_server<R>(reader: &mut R) -> Result<ServerMessage>
where
  R: AsyncRead + Unpin,
{
  read_server_with_options(reader, remote_reader_options()).await
}

pub(crate) async fn read_local_server<R>(
  reader: &mut R,
) -> Result<ServerMessage>
where
  R: AsyncRead + Unpin,
{
  read_server_with_options(reader, local_reader_options()).await
}

async fn read_server_with_options<R>(
  reader: &mut R,
  options: ReaderOptions,
) -> Result<ServerMessage>
where
  R: AsyncRead + Unpin,
{
  let message = capnp_futures::serialize::read_message(reader, options).await?;
  let root = message.get_root::<worker_capnp::server_message::Reader>()?;
  match root.which()? {
    worker_capnp::server_message::Which::Ready(()) => Ok(ServerMessage::Ready),
    worker_capnp::server_message::Which::Event(event) => {
      Ok(ServerMessage::Event(Box::new(read_event(event?)?)))
    },
    worker_capnp::server_message::Which::Status(status) => {
      let status = match status? {
        worker_capnp::WorkerStatus::Ready => WorkerStatus::Ready,
        worker_capnp::WorkerStatus::Restart => WorkerStatus::Restart,
      };
      Ok(ServerMessage::Status(status))
    },
    worker_capnp::server_message::Which::Error(error) => {
      Ok(ServerMessage::Error(error?.to_string()?))
    },
  }
}

fn remote_reader_options() -> ReaderOptions {
  ReaderOptions {
    traversal_limit_in_words: Some(REMOTE_TRAVERSAL_LIMIT_WORDS),
    nesting_limit:            NESTING_LIMIT,
  }
}

fn local_reader_options() -> ReaderOptions {
  ReaderOptions {
    traversal_limit_in_words: Some(LOCAL_TRAVERSAL_LIMIT_WORDS),
    nesting_limit:            NESTING_LIMIT,
  }
}

pub(crate) fn expect_ready(message: ServerMessage, label: &str) -> Result<()> {
  match message {
    ServerMessage::Ready => Ok(()),
    ServerMessage::Error(error) => {
      bail!("remote worker {label} failed: {error}")
    },
    other => {
      bail!("remote worker {label} sent unexpected handshake: {other:?}")
    },
  }
}

fn set_worker_config(
  mut builder: worker_capnp::worker_config::Builder<'_>,
  config: &WorkerConfig,
) -> Result<()> {
  set_input(builder.reborrow().init_input(), &config.input);

  let mut auto_args = builder
    .reborrow()
    .init_auto_args(list_len(config.auto_args.len(), "auto args")?);
  for (index, (name, arg)) in config.auto_args.iter().enumerate() {
    let mut item = auto_args.reborrow().get(list_index(index)?);
    item.set_name(name);
    match arg {
      AutoArg::Expr(expr) => item.set_expr(expr),
      AutoArg::Str(value) => item.set_str(value),
    }
  }

  builder.set_force_recurse(config.force_recurse);
  set_text_opt(
    builder.reborrow().init_gc_roots_dir(),
    config
      .gc_roots_dir
      .as_ref()
      .map(|path| path.to_string_lossy().into_owned())
      .as_deref(),
  );
  builder.set_max_memory_size(config.max_memory_size as u64);
  builder.set_item_timeout_seconds(config.item_timeout_seconds);
  builder.set_meta(config.meta);
  builder.set_show_input_drvs(config.show_input_drvs);
  set_pairs(
    builder.reborrow().init_override_inputs(list_len(
      config.override_inputs.len(),
      "override inputs",
    )?),
    &config.override_inputs,
  )?;
  set_pairs(
    builder
      .reborrow()
      .init_nix_options(list_len(config.nix_options.len(), "nix options")?),
    &config.nix_options,
  )?;
  set_text_opt(
    builder.reborrow().init_locked_flake_json(),
    config.locked_flake_json.as_deref(),
  );
  Ok(())
}

fn read_worker_config(
  reader: worker_capnp::worker_config::Reader<'_>,
) -> Result<WorkerConfig> {
  Ok(WorkerConfig {
    input:                read_input(reader.get_input()?)?,
    auto_args:            read_auto_args(reader.get_auto_args()?)?,
    force_recurse:        reader.get_force_recurse(),
    gc_roots_dir:         read_text_opt(reader.get_gc_roots_dir()?)?
      .map(PathBuf::from),
    max_memory_size:      reader
      .get_max_memory_size()
      .try_into()
      .context("maxMemorySize exceeds this platform's usize")?,
    item_timeout_seconds: read_item_timeout_seconds(reader),
    meta:                 reader.get_meta(),
    show_input_drvs:      reader.get_show_input_drvs(),
    override_inputs:      read_pairs(reader.get_override_inputs()?)?,
    nix_options:          read_pairs(reader.get_nix_options()?)?,
    locked_flake_json:    read_text_opt(reader.get_locked_flake_json()?)?,
  })
}

fn read_item_timeout_seconds(
  reader: worker_capnp::worker_config::Reader<'_>,
) -> u64 {
  match reader.get_item_timeout_seconds() {
    0 => DEFAULT_ITEM_TIMEOUT_SECONDS,
    seconds => seconds,
  }
}

fn set_input(mut builder: worker_capnp::input::Builder<'_>, input: &Input) {
  match input {
    Input::Flake(value) => builder.set_flake(value),
    Input::Expr(value) => builder.set_expr(value),
    Input::File(path) => builder.set_file(path.to_string_lossy()),
  }
}

fn read_input(reader: worker_capnp::input::Reader<'_>) -> Result<Input> {
  Ok(match reader.which()? {
    worker_capnp::input::Which::Flake(value) => {
      Input::Flake(value?.to_string()?)
    },
    worker_capnp::input::Which::Expr(value) => Input::Expr(value?.to_string()?),
    worker_capnp::input::Which::File(value) => {
      Input::File(PathBuf::from(value?.to_str()?))
    },
  })
}

fn read_auto_args(
  list: capnp::struct_list::Reader<'_, worker_capnp::auto_arg::Owned>,
) -> Result<Vec<(String, AutoArg)>> {
  let mut out = Vec::with_capacity(list.len() as usize);
  for index in 0..list.len() {
    let item = list.get(index);
    let name = item.get_name()?.to_string()?;
    let value = match item.which()? {
      worker_capnp::auto_arg::Which::Expr(expr) => {
        AutoArg::Expr(expr?.to_string()?)
      },
      worker_capnp::auto_arg::Which::Str(value) => {
        AutoArg::Str(value?.to_string()?)
      },
    };
    out.push((name, value));
  }
  Ok(out)
}

fn set_event(
  mut builder: worker_capnp::event::Builder<'_>,
  event: &Event,
) -> Result<()> {
  match event {
    Event::Derivation(derivation) => {
      set_derivation(builder.reborrow().init_derivation(), derivation)?;
    },
    Event::AttrSet {
      attr,
      attr_path,
      attrs,
    } => {
      let mut attr_set = builder.reborrow().init_attr_set();
      attr_set.set_attr(attr);
      set_text_list(
        attr_set
          .reborrow()
          .init_attr_path(list_len(attr_path.len(), "attr path")?),
        attr_path,
      )?;
      set_text_list(
        attr_set
          .reborrow()
          .init_attrs(list_len(attrs.len(), "attr set children")?),
        attrs,
      )?;
    },
    Event::Error(error) => {
      set_eval_error(builder.reborrow().init_error(), error)?
    },
  }
  Ok(())
}

fn read_event(reader: worker_capnp::event::Reader<'_>) -> Result<Event> {
  Ok(match reader.which()? {
    worker_capnp::event::Which::Derivation(derivation) => {
      Event::Derivation(read_derivation(derivation?)?)
    },
    worker_capnp::event::Which::AttrSet(attr_set) => {
      let attr_set = attr_set?;
      Event::AttrSet {
        attr:      attr_set.get_attr()?.to_string()?,
        attr_path: read_text_list(attr_set.get_attr_path()?)?,
        attrs:     read_text_list(attr_set.get_attrs()?)?,
      }
    },
    worker_capnp::event::Which::Error(error) => {
      Event::Error(read_eval_error(error?)?)
    },
  })
}

fn set_derivation(
  mut builder: worker_capnp::derivation::Builder<'_>,
  derivation: &Derivation,
) -> Result<()> {
  builder.set_attr(&derivation.attr);
  set_text_list(
    builder.reborrow().init_attr_path(list_len(
      derivation.attr_path.len(),
      "derivation attr path",
    )?),
    &derivation.attr_path,
  )?;
  builder.set_name(&derivation.name);
  builder.set_system(&derivation.system);
  builder.set_drv_path(&derivation.drv_path);

  let mut outputs = builder
    .reborrow()
    .init_outputs(list_len(derivation.outputs.len(), "derivation outputs")?);
  for (index, (name, path)) in derivation.outputs.iter().enumerate() {
    let mut output = outputs.reborrow().get(list_index(index)?);
    output.set_name(name);
    match path {
      Some(path) => output.set_path(path),
      None => output.set_absent(()),
    }
  }

  let meta_json = derivation
    .meta
    .as_ref()
    .map(serde_json::to_string)
    .transpose()?;
  set_text_opt(builder.reborrow().init_meta_json(), meta_json.as_deref());

  let mut input_drvs = builder
    .reborrow()
    .init_input_drvs(list_len(derivation.input_drvs.len(), "input drvs")?);
  for (index, (drv_path, outputs)) in derivation.input_drvs.iter().enumerate() {
    let mut input_drv = input_drvs.reborrow().get(list_index(index)?);
    input_drv.set_drv_path(drv_path);
    input_drv.set_value_json(&serde_json::to_string(outputs)?);
  }

  match &derivation.constituents {
    Some(constituents) => {
      set_text_list(
        builder
          .reborrow()
          .init_constituents()
          .init_some(list_len(constituents.len(), "constituents")?),
        constituents,
      )?;
    },
    None => builder.reborrow().init_constituents().set_none(()),
  }
  set_text_opt(
    builder.reborrow().init_gc_root_error(),
    derivation.gc_root_error.as_deref(),
  );
  Ok(())
}

fn read_derivation(
  reader: worker_capnp::derivation::Reader<'_>,
) -> Result<Derivation> {
  let outputs = reader.get_outputs()?;
  let mut output_map = BTreeMap::new();
  for index in 0..outputs.len() {
    let output = outputs.get(index);
    let value = match output.which()? {
      worker_capnp::output::Which::Absent(()) => None,
      worker_capnp::output::Which::Path(path) => Some(path?.to_string()?),
    };
    output_map.insert(output.get_name()?.to_string()?, value);
  }

  let input_drvs = reader.get_input_drvs()?;
  let mut input_drv_map = BTreeMap::new();
  for index in 0..input_drvs.len() {
    let input_drv = input_drvs.get(index);
    input_drv_map.insert(
      input_drv.get_drv_path()?.to_string()?,
      serde_json::from_str::<Vec<String>>(
        input_drv.get_value_json()?.to_str()?,
      )
      .context("parsing inputDrv outputs")?,
    );
  }

  Ok(Derivation {
    attr:          reader.get_attr()?.to_string()?,
    attr_path:     read_text_list(reader.get_attr_path()?)?,
    name:          reader.get_name()?.to_string()?,
    system:        reader.get_system()?.to_string()?,
    drv_path:      reader.get_drv_path()?.to_string()?,
    outputs:       output_map,
    meta:          read_text_opt(reader.get_meta_json()?)?
      .map(|json| serde_json::from_str(&json))
      .transpose()
      .context("parsing meta JSON value")?,
    input_drvs:    input_drv_map,
    constituents:  read_text_list_opt(reader.get_constituents()?)?,
    gc_root_error: read_text_opt(reader.get_gc_root_error()?)?,
  })
}

fn set_eval_error(
  mut builder: worker_capnp::eval_error::Builder<'_>,
  error: &EvalError,
) -> Result<()> {
  builder.set_attr(&error.attr);
  set_text_list(
    builder
      .reborrow()
      .init_attr_path(list_len(error.attr_path.len(), "error attr path")?),
    &error.attr_path,
  )?;
  builder.set_error(&error.error);
  builder.set_fatal(error.fatal);
  Ok(())
}

fn read_eval_error(
  reader: worker_capnp::eval_error::Reader<'_>,
) -> Result<EvalError> {
  Ok(EvalError {
    attr:      reader.get_attr()?.to_string()?,
    attr_path: read_text_list(reader.get_attr_path()?)?,
    error:     reader.get_error()?.to_string()?,
    fatal:     reader.get_fatal(),
  })
}

fn set_pairs(
  mut builder: capnp::struct_list::Builder<
    '_,
    worker_capnp::string_pair::Owned,
  >,
  pairs: &[(String, String)],
) -> Result<()> {
  for (index, (key, value)) in pairs.iter().enumerate() {
    let mut item = builder.reborrow().get(list_index(index)?);
    item.set_key(key);
    item.set_value(value);
  }
  Ok(())
}

fn read_pairs(
  list: capnp::struct_list::Reader<'_, worker_capnp::string_pair::Owned>,
) -> Result<Vec<(String, String)>> {
  let mut out = Vec::with_capacity(list.len() as usize);
  for index in 0..list.len() {
    let item = list.get(index);
    out.push((item.get_key()?.to_string()?, item.get_value()?.to_string()?));
  }
  Ok(out)
}

fn set_text_opt(
  mut builder: worker_capnp::text_opt::Builder<'_>,
  value: Option<&str>,
) {
  match value {
    Some(value) => builder.set_some(value),
    None => builder.set_none(()),
  }
}

fn read_text_opt(
  reader: worker_capnp::text_opt::Reader<'_>,
) -> Result<Option<String>> {
  Ok(match reader.which()? {
    worker_capnp::text_opt::Which::None(()) => None,
    worker_capnp::text_opt::Which::Some(value) => Some(value?.to_string()?),
  })
}

fn read_text_list_opt(
  reader: worker_capnp::text_list_opt::Reader<'_>,
) -> Result<Option<Vec<String>>> {
  Ok(match reader.which()? {
    worker_capnp::text_list_opt::Which::None(()) => None,
    worker_capnp::text_list_opt::Which::Some(value) => {
      Some(read_text_list(value?)?)
    },
  })
}

fn set_text_list(
  mut builder: capnp::text_list::Builder<'_>,
  values: &[String],
) -> Result<()> {
  for (index, value) in values.iter().enumerate() {
    builder.set(list_index(index)?, value);
  }
  Ok(())
}

fn read_text_list(reader: capnp::text_list::Reader<'_>) -> Result<Vec<String>> {
  let mut out = Vec::with_capacity(reader.len() as usize);
  for index in 0..reader.len() {
    out.push(reader.get(index)?.to_string()?);
  }
  Ok(out)
}

fn list_len(len: usize, field: &str) -> Result<u32> {
  u32::try_from(len)
    .with_context(|| format!("{field} length exceeds Cap'n Proto list limit"))
}

fn list_index(index: usize) -> Result<u32> {
  u32::try_from(index).context("Cap'n Proto list index exceeds u32")
}

#[cfg(test)]
mod tests {
  use std::collections::BTreeMap;

  use capnp::message::Builder;
  use futures_util::io::Cursor;

  use super::*;

  #[test]
  fn setup_rejects_mismatched_protocol_version() {
    tokio::runtime::Builder::new_current_thread()
      .enable_io()
      .build()
      .unwrap()
      .block_on(async {
        let mut message = Builder::new_default();
        message
          .init_root::<worker_capnp::client_message::Builder>()
          .init_setup()
          .set_protocol_version(WORKER_PROTOCOL_VERSION + 1);

        let mut bytes = Cursor::new(Vec::new());
        capnp_futures::serialize::write_message(&mut bytes, &message)
          .await
          .unwrap();
        bytes.set_position(0);

        let error = read_client(&mut bytes).await.unwrap_err().to_string();

        assert!(error.contains("unsupported worker protocol version"));
      });
  }

  #[test]
  fn setup_preserves_item_timeout_seconds() {
    tokio::runtime::Builder::new_current_thread()
      .enable_io()
      .build()
      .unwrap()
      .block_on(async {
        let mut config = WorkerConfig::from(&crate::Config::default());
        config.item_timeout_seconds = 3_600;
        config.locked_flake_json = Some(r#"{"lockFile":{}}"#.into());
        let mut bytes = Cursor::new(Vec::new());

        write_client(&mut bytes, &ClientMessage::Setup {
          config,
          token: None,
          expected_store_dir: None,
        })
        .await
        .unwrap();
        bytes.set_position(0);

        let ClientMessage::Setup { config, .. } =
          read_client(&mut bytes).await.unwrap()
        else {
          panic!("expected setup");
        };

        assert_eq!(config.item_timeout_seconds, 3_600);
        assert_eq!(
          config.locked_flake_json.as_deref(),
          Some(r#"{"lockFile":{}}"#)
        );
      });
  }

  #[test]
  fn local_server_reads_use_larger_traversal_limit_than_remote_reads() {
    tokio::runtime::Builder::new_current_thread()
      .enable_io()
      .build()
      .unwrap()
      .block_on(async {
        assert!(
          local_reader_options().traversal_limit_in_words
            > remote_reader_options().traversal_limit_in_words
        );

        let message =
          ServerMessage::Event(Box::new(Event::Derivation(Derivation {
            attr:          "huge".into(),
            attr_path:     vec!["huge".into()],
            name:          "huge".into(),
            system:        "x86_64-linux".into(),
            drv_path:      "/nix/store/huge.drv".into(),
            outputs:       BTreeMap::new(),
            meta:          Some(serde_json::json!({
              "description": "x".repeat(64 * 1024)
            })),
            input_drvs:    BTreeMap::new(),
            constituents:  None,
            gc_root_error: None,
          })));

        let mut bytes = Cursor::new(Vec::new());
        write_server(&mut bytes, &message).await.unwrap();

        let mut tiny_options = remote_reader_options();
        tiny_options.traversal_limit_in_words(Some(128));

        bytes.set_position(0);
        let error = read_server_with_options(&mut bytes, tiny_options)
          .await
          .unwrap_err()
          .to_string();
        assert!(error.contains("too large"), "{error}");

        bytes.set_position(0);
        let ServerMessage::Event(event) =
          read_local_server(&mut bytes).await.unwrap()
        else {
          panic!("expected event");
        };
        let Event::Derivation(derivation) = *event else {
          panic!("expected derivation");
        };
        assert_eq!(derivation.attr, "huge");
      });
  }
}
