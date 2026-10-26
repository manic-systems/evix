//! Forks the extra local workers from the first one once it has evaluated a
//! few items, so they share its evaluated heap.

use std::{
  env,
  ffi::OsString,
  fs::{self, File},
  mem,
  os::{
    fd::{AsRawFd as _, FromRawFd as _, OwnedFd, RawFd},
    unix::ffi::OsStringExt as _,
  },
  sync::Arc,
  time::{Duration, Instant},
};

use misstep::{OptionExt as _, Result, ResultExt as _, bail, report};
use nix_bindings::{Context, Store};
use tracing::{debug, warn};

pub(crate) const FORK_FDS_ENV: &str = "EVIX_FORK_FDS";
const ITEMS_PER_FORK: usize = 5;
const TIME_PER_FORK: Duration = Duration::from_millis(200);

pub(crate) struct ForkSlot {
  pub(crate) socket: OwnedFd,
  pub(crate) stderr: OwnedFd,
}

pub(crate) struct Forker {
  slots:         Vec<ForkSlot>,
  forked:        usize,
  started:       Instant,
  children:      Vec<libc::pid_t>,
  max_memory_kb: usize,
}

impl ForkSlot {
  pub(crate) fn env_value(slots: &[Self]) -> String {
    slots
      .iter()
      .map(|slot| {
        format!("{}:{}", slot.socket.as_raw_fd(), slot.stderr.as_raw_fd())
      })
      .collect::<Vec<_>>()
      .join(",")
  }
}

impl Forker {
  pub(crate) fn from_env(store: &Store, max_memory_mb: usize) -> Result<Self> {
    let mut slots = inherited_slots()?;
    if !slots.is_empty() && !is_daemon_store(store) {
      debug!("store is not the Nix daemon, not forking workers");
      slots.clear();
    }
    Ok(Self {
      slots,
      forked: 0,
      started: Instant::now(),
      children: Vec::new(),
      max_memory_kb: max_memory_mb.saturating_mul(1024),
    })
  }

  pub(crate) fn start_clock(&mut self) {
    self.started = Instant::now();
  }

  pub(crate) fn reap(&mut self) {
    self.children.retain(|&pid| {
      // SAFETY: `pid` is an unreaped child of this process.
      unsafe { libc::waitpid(pid, std::ptr::null_mut(), libc::WNOHANG) == 0 }
    });
  }

  pub(crate) fn fork_due(
    &mut self,
    ctx: &Arc<Context>,
    completed: usize,
    max_rss_kb: usize,
  ) -> Result<Option<ForkSlot>> {
    while !self.slots.is_empty() && self.is_due(completed) {
      if let Some(hazard) = self.fork_hazard(max_rss_kb)? {
        warn!(hazard, "not forking workers");
        self.slots.clear();
        return Ok(None);
      }

      let slot = self.slots.remove(0);
      self.forked += 1;

      // SAFETY: `fork_hazard` found no threads besides libgc's markers, and
      // libgc registers `pthread_atfork` handlers for those.
      let pid = unsafe { libc::fork() };
      if pid < 0 {
        warn!(error = %std::io::Error::last_os_error(), "forking a worker failed");
        return Ok(None);
      }
      if pid > 0 {
        self.children.push(pid);
        continue;
      }

      self.slots.clear();
      self.children.clear();
      replace_daemon_connections(ctx)?;
      return Ok(Some(slot));
    }
    Ok(None)
  }

  fn is_due(&self, completed: usize) -> bool {
    let next = u32::try_from(self.forked + 1).unwrap_or(u32::MAX);
    completed >= ITEMS_PER_FORK.saturating_mul(self.forked + 1)
      || self.started.elapsed() >= TIME_PER_FORK.saturating_mul(next)
  }

  fn fork_hazard(&self, max_rss_kb: usize) -> Result<Option<&'static str>> {
    if max_rss_kb > self.max_memory_kb / 2 {
      return Ok(Some("worker is past half its memory limit"));
    }

    let main = std::process::id().to_string();
    for entry in fs::read_dir("/proc/self/task").context("listing threads")? {
      let entry = entry?;
      if entry.file_name() == main.as_str() {
        continue;
      }
      let comm = fs::read_to_string(entry.path().join("comm"))
        .context("reading a thread name")?;
      if !comm.starts_with("GC-marker-") {
        return Ok(Some("worker runs a thread other than libgc's markers"));
      }
    }

    for entry in fs::read_dir("/proc/self/fd").context("listing descriptors")? {
      let target = fs::read_link(entry?.path()).unwrap_or_default();
      if target.extension().is_some_and(|ext| ext == "sqlite") {
        return Ok(Some("worker has a SQLite database open"));
      }
    }
    Ok(None)
  }
}

fn inherited_slots() -> Result<Vec<ForkSlot>> {
  let Some(fds) = env::var_os(FORK_FDS_ENV) else {
    return Ok(Vec::new());
  };
  // SAFETY: the worker is still single-threaded here.
  unsafe { env::remove_var(FORK_FDS_ENV) };
  let fds = fds
    .into_string()
    .map_err(|fds| report!("{FORK_FDS_ENV} is not UTF-8: {fds:?}"))?;
  fds
    .split(',')
    .map(|slot| {
      let (socket, stderr) = slot
        .split_once(':')
        .with_context(|| format!("parsing {FORK_FDS_ENV} slot {slot:?}"))?;
      Ok(ForkSlot {
        socket: inherited_fd(socket)?,
        stderr: inherited_fd(stderr)?,
      })
    })
    .collect()
}

fn inherited_fd(fd: &str) -> Result<OwnedFd> {
  let fd = fd
    .parse::<RawFd>()
    .with_context(|| format!("parsing {FORK_FDS_ENV}"))?;
  // SAFETY: `fcntl` takes no pointers.
  if unsafe { libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC) } < 0 {
    return Err(std::io::Error::last_os_error())
      .with_context(|| format!("marking fork slot {fd} close-on-exec"))
      .erase();
  }
  // SAFETY: the master opened this descriptor for this process alone, and
  // `fcntl` just confirmed it is open.
  Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

fn is_daemon_store(store: &Store) -> bool {
  store
    .uri()
    .is_ok_and(|uri| uri == "daemon" || uri.starts_with("unix://"))
}

fn replace_daemon_connections(ctx: &Arc<Context>) -> Result<()> {
  let mut pending = Option::<Vec<RawFd>>::None;
  loop {
    let before = unix_peers()?;
    let store =
      Store::open(ctx, None).context("opening a daemon connection")?;
    store.version().context("connecting to the Nix daemon")?;

    let fresh = unix_peers()?
      .into_iter()
      .filter(|(fd, _)| before.iter().all(|(old, _)| old != fd))
      .collect::<Vec<(i32, OsString)>>();
    let [(fresh_fd, peer)] = fresh.as_slice() else {
      bail!("expected one new daemon connection, found {}", fresh.len());
    };

    let targets = pending.get_or_insert_with(|| {
      before
        .iter()
        .filter(|(_, path)| path == peer)
        .map(|(fd, _)| *fd)
        .collect()
    });
    let Some(target) = targets.pop() else {
      debug!("replaced inherited daemon connections");
      return Ok(());
    };

    // SAFETY: both descriptors are open, and `store` is leaked so the fresh
    // socket outlives every use of `target`.
    if unsafe { libc::dup2(*fresh_fd, target) } < 0 {
      return Err(std::io::Error::last_os_error())
        .context("replacing an inherited daemon connection")
        .erase();
    }

    mem::forget(store);
  }
}

fn unix_peers() -> Result<Vec<(RawFd, OsString)>> {
  let mut peers = Vec::new();
  for entry in fs::read_dir("/proc/self/fd").context("listing descriptors")? {
    let name = entry?.file_name();
    let Some(fd) = name.to_str().and_then(|name| name.parse::<RawFd>().ok())
    else {
      continue;
    };

    if let Some(path) = unix_peer(fd) {
      peers.push((fd, path));
    }
  }
  Ok(peers)
}

fn unix_peer(fd: RawFd) -> Option<OsString> {
  // SAFETY: `sockaddr_un` is plain data, and `getpeername` writes at most
  // `len` bytes into it.
  let mut addr = unsafe { mem::zeroed::<libc::sockaddr_un>() };
  let mut len = mem::size_of::<libc::sockaddr_un>() as libc::socklen_t;

  let rc =
    unsafe { libc::getpeername(fd, (&raw mut addr).cast(), &raw mut len) };
  if rc != 0 || addr.sun_family != libc::AF_UNIX as libc::sa_family_t {
    return None;
  }

  let path = addr
    .sun_path
    .iter()
    .take_while(|&&byte| byte != 0)
    .map(|&byte| byte as u8)
    .collect::<Vec<u8>>();
  (!path.is_empty()).then(|| OsString::from_vec(path))
}

pub(crate) fn detach_stdio(stderr: OwnedFd) -> Result<()> {
  let null = File::options()
    .read(true)
    .write(true)
    .open("/dev/null")
    .context("opening /dev/null")?;
  for (source, target) in [
    (null.as_raw_fd(), libc::STDIN_FILENO),
    (null.as_raw_fd(), libc::STDOUT_FILENO),
    (stderr.as_raw_fd(), libc::STDERR_FILENO),
  ] {
    // SAFETY: both descriptors are open, and `dup2` replaces `target`.
    if unsafe { libc::dup2(source, target) } < 0 {
      return Err(std::io::Error::last_os_error())
        .context("redirecting worker stdio")
        .erase();
    }
  }
  Ok(())
}
