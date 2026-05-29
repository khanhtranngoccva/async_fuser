//! Filesystem session
//!
//! A session runs a filesystem implementation while it is being mounted to a specific mount
//! point. A session begins by mounting the filesystem and ends by unmounting it. While the
//! filesystem is mounted, the session loop receives, dispatches and replies to kernel requests
//! for filesystem operations under its mount point.

use libc::ENODEV;
use std::borrow::Cow;
use std::fmt::Debug;
use std::io;
use std::num::NonZero;
use std::ops::Deref;
use std::os::fd::AsFd;
use std::os::fd::BorrowedFd;
use std::os::fd::OwnedFd;
use std::path::Path;
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::runtime::RuntimeFlavor;
use tokio::task::JoinHandle;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use log::debug;
use log::error;
use log::info;
use nix::unistd::Uid;
use nix::unistd::geteuid;
use xutex::Mutex;

use crate::Errno;
use crate::Filesystem;
use crate::KernelConfig;
use crate::MountOption;
use crate::ReplyEmpty;
use crate::Request;
use crate::UnmountOption;
use crate::channel::Channel;
use crate::channel::ChannelSender;
use crate::dev_fuse::DevFuse;
use crate::ll;
use crate::ll::Operation;
use crate::ll::ResponseErrno;
use crate::ll::Version;
use crate::ll::flags::init_flags::InitFlags;
use crate::ll::fuse_abi as abi;
use crate::mnt::Mount;
use crate::mnt::drop_umount_flags;
use crate::mnt::mount_options::Config;
use crate::mnt::mount_options::check_option_conflicts;
use crate::notify::Notifier;
use crate::read_buf::FuseReadBuf;
use crate::reply::Reply;
use crate::reply::ReplyRaw;
use crate::reply::ReplySender;
use crate::request::CancelCookie;
use crate::request::CancelManager;
use crate::request::RequestWithSender;
use crate::runtime;
use crate::runtime::DroppableRuntime;

/// The max size of write requests from the kernel. The absolute minimum is 4k,
/// FUSE recommends at least 128k, max 16M. The FUSE default is 16M on macOS
/// and 128k on other systems.
pub(crate) const MAX_WRITE_SIZE: usize = 16 * 1024 * 1024;

#[derive(Default, Debug, Eq, PartialEq, Clone, Copy)]
/// How requests should be filtered based on the calling UID.
pub enum SessionACL {
    /// Allow requests from any user. Corresponds to the `allow_other` mount option.
    All,
    /// Allow requests from root. Corresponds to the `allow_root` mount option.
    RootAndOwner,
    /// Allow requests from the owning UID. This is FUSE's default mode of operation.
    #[default]
    Owner,
}

impl SessionACL {
    /// Returns the mount option string for kernel/fusermount/libfuse paths.
    /// Both `All` and `RootAndOwner` map to `allow_other` - the kernel only
    /// understands `allow_other`, and fuser enforces the root-only restriction internally.
    #[allow(unused)]
    pub(crate) fn to_mount_option(self) -> Option<&'static str> {
        match self {
            SessionACL::All | SessionACL::RootAndOwner => Some("allow_other"),
            SessionACL::Owner => None,
        }
    }
}

/// Runtime strategy for the session. Controls how internal tasks are spawned.
///
/// In the optimal scenario, it tries to use the current thread's runtime handle if it is a multi-threaded runtime. Otherwise, it will use a managed strategy. However, applications should manually configure this enum to avoid unpredictable behavior.
#[derive(Debug, Clone)]
pub enum RuntimeStrategy {
    /// Use a caller-managed handle to spawn the session. This is the recommended option because it allows scaling to multiple filesystems without risk of thread oversubscription.
    ///
    /// The handle must point to a multi-threaded Tokio runtime to avoid a deadlock. Passing a current-thread handle will return an error.
    Unmanaged {
        /// Number of event loop workers to spawn on the runtime. 1 worker per filesystem is by default.
        n_event_loop_workers: Option<NonZero<usize>>,
        /// Handle to the runtime to use.
        handle: tokio::runtime::Handle,
    },
    /// Spawn a managed runtime for the session. This option is not recommended because it causes thread oversubscription if multiple filesystems are used.
    Managed {
        /// Number of event loop workers to spawn on the runtime. 1 worker per filesystem is by default.
        n_event_loop_workers: Option<NonZero<usize>>,
        /// Number of handler workers to spawn on the runtime. 1 worker per filesystem is by default.
        n_handler_workers: Option<NonZero<usize>>,
    },
}

impl Default for RuntimeStrategy {
    fn default() -> Self {
        let handle = tokio::runtime::Handle::try_current();
        if let Ok(handle) = handle
            && handle.runtime_flavor() == RuntimeFlavor::MultiThread
        {
            RuntimeStrategy::Unmanaged {
                n_event_loop_workers: None,
                handle,
            }
        } else {
            log::warn!(
                "cannot automatically pick a multi-threaded runtime handle, falling back to managed strategy"
            );
            RuntimeStrategy::Managed {
                n_event_loop_workers: None,
                n_handler_workers: std::thread::available_parallelism().ok(),
            }
        }
    }
}

impl RuntimeStrategy {
    /// Verify that the runtime strategy is valid.
    pub(crate) fn verify(&self) -> Result<(), io::Error> {
        if !cfg!(target_os = "linux") && self.event_loop_workers().get() != 1 {
            return Err(io::Error::other(
                "n_event_loop_workers != 1 is only supported on Linux",
            ));
        }
        match self {
            RuntimeStrategy::Unmanaged { handle, .. } => {
                if handle.runtime_flavor() != RuntimeFlavor::MultiThread {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "unmanaged runtime strategy must be used with a multi-threaded runtime",
                    ));
                }
            }
            RuntimeStrategy::Managed { .. } => {}
        }
        Ok(())
    }

    /// Get the number of event loop workers for the runtime strategy.
    pub(crate) fn event_loop_workers(&self) -> NonZero<usize> {
        match self {
            RuntimeStrategy::Unmanaged {
                n_event_loop_workers,
                ..
            } => n_event_loop_workers.unwrap_or(NonZero::new(1).unwrap()),
            RuntimeStrategy::Managed {
                n_event_loop_workers,
                ..
            } => n_event_loop_workers.unwrap_or(NonZero::new(1).unwrap()),
        }
    }

    /// Build the managed runtime for the runtime strategy if necessary and return a valid handle to a multi-threaded runtime.
    pub(crate) fn build(&self) -> Result<(Handle, Option<DroppableRuntime>), io::Error> {
        match self {
            RuntimeStrategy::Unmanaged { handle, .. } => Ok((handle.clone(), None)),
            RuntimeStrategy::Managed {
                n_event_loop_workers,
                n_handler_workers,
                ..
            } => {
                let n_event_loop_workers = n_event_loop_workers.map(NonZero::get).unwrap_or(1);
                let n_handler_workers = n_handler_workers.map(NonZero::get).unwrap_or(1);
                let runtime = DroppableRuntime::new(
                    "afuser-hnd",
                    n_event_loop_workers + n_handler_workers,
                    false,
                )?;
                Ok((runtime.handle().clone(), Some(runtime)))
            }
        }
    }
}

/// Calls `destroy` on drop.
#[derive(Debug)]
pub(crate) struct FilesystemHolder<FS: Filesystem> {
    pub(crate) fs: Option<FS>,
}

impl<FS: Filesystem> FilesystemHolder<FS> {
    pub(crate) fn new(fs: FS) -> Self {
        Self { fs: Some(fs) }
    }
}

impl<FS: Filesystem> FilesystemHolder<FS> {
    fn destroy(&mut self) {
        if let Some(mut fs) = self.fs.take() {
            runtime::execute_future_from_sync(async move { fs.destroy().await });
        }
    }
}

impl<FS: Filesystem> Drop for FilesystemHolder<FS> {
    fn drop(&mut self) {
        self.destroy();
    }
}

impl<FS: Filesystem> Deref for FilesystemHolder<FS> {
    type Target = FS;

    fn deref(&self) -> &Self::Target {
        self.fs.as_ref().expect("filesystem must be initialized")
    }
}

struct UmountOnDrop {
    mount: Arc<Mutex<Option<Mount>>>,
}

impl Debug for UmountOnDrop {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UmountOnDrop")
            .field("mount", &*self.mount.lock())
            .finish()
    }
}

impl UmountOnDrop {
    async fn umount(&self, flags: &[UnmountOption]) -> io::Result<()> {
        let mut guard = self.mount.lock();
        let mount = match guard.take() {
            Some(mount) => mount,
            None => return Ok(()),
        };
        mount.umount(flags).await.map_err(|(mount, error)| {
            *guard = mount;
            error
        })?;
        Ok(())
    }
}

impl Drop for UmountOnDrop {
    fn drop(&mut self) {
        // Use the internal mount drop implementation.
        let mut guard = self.mount.lock();
        drop(guard.take());
    }
}

/// The session data structure
#[derive(Debug)]
pub struct Session<FS: Filesystem> {
    /// Filesystem operation implementations. None after `destroy` called.
    pub(crate) filesystem: FilesystemHolder<FS>,
    /// Communication channel to the kernel driver
    pub(crate) ch: Channel,
    /// Handle to the mount.  Dropping this unmounts.
    mount: UmountOnDrop,
    /// User that launched the fuser process
    pub(crate) session_owner: Uid,
    /// FUSE protocol version, as reported by the kernel.
    /// The field is set to `Some` when the init message is received.
    pub(crate) proto_version: Option<Version>,
    /// Configuration for the session
    pub(crate) config: Config,
    /// CancellationToken for the session. When the token is triggered, all event loops terminate.
    pub(crate) cancellation_token: CancellationToken,
    /// Handle to the runtime for the session.
    pub(crate) handle: Handle,
    /// Managed runtime for the session.
    pub(crate) managed_runtime: Option<DroppableRuntime>,
}

impl<FS: Filesystem> AsFd for Session<FS> {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.ch.as_fd()
    }
}

impl<FS: Filesystem> Session<FS> {
    /// Create a new session by mounting the given filesystem to the given mountpoint
    /// # Errors
    /// Returns an error if the options are incorrect, or if the fuse device can't be mounted.
    pub async fn new<P: AsRef<Path>>(
        filesystem: FS,
        mountpoint: P,
        options: &Config,
    ) -> io::Result<Session<FS>> {
        check_option_conflicts(options)?;
        options.runtime_strategy.verify()?;

        // Ensure the internal dependencies are ready first.
        let token = CancellationToken::new();
        let (handle, managed_runtime) = options.runtime_strategy.build()?;

        // Perform the mount.
        let mountpoint = mountpoint.as_ref();
        info!("Mounting {}", mountpoint.display());
        // If AutoUnmount is requested, but not AllowRoot or AllowOther, return an error
        // because fusermount needs allow_root or allow_other to handle the auto_unmount option
        if options.mount_options.contains(&MountOption::AutoUnmount)
            && options.acl == SessionACL::Owner
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("auto_unmount requires acl != Owner, got: {:?}", options.acl),
            ));
        }
        let (file, mount) = Mount::new(mountpoint, &options.mount_options, options.acl).await?;
        let ch = Channel::new(file);

        let mut session = Session {
            filesystem: FilesystemHolder::new(filesystem),
            ch,
            mount: UmountOnDrop {
                mount: Arc::new(Mutex::new(Some(mount))),
            },
            session_owner: geteuid(),
            proto_version: None,
            config: options.clone(),
            cancellation_token: token,
            handle,
            managed_runtime,
        };

        session.handshake().await?;
        Ok(session)
    }

    /// Wrap an existing /dev/fuse file descriptor. This doesn't mount the
    /// filesystem anywhere; that must be done separately.
    pub async fn from_fd(filesystem: FS, fd: OwnedFd, config: Config) -> io::Result<Self> {
        config.runtime_strategy.verify()?;
        let (handle, managed_runtime) = config.runtime_strategy.build()?;

        let ch = Channel::new(Arc::new(DevFuse::try_from_fd(fd).await?));
        let mut session = Session {
            filesystem: FilesystemHolder::new(filesystem),
            ch,
            mount: UmountOnDrop {
                mount: Arc::new(Mutex::new(None)),
            },
            session_owner: geteuid(),
            proto_version: None,
            config,
            cancellation_token: CancellationToken::new(),
            handle,
            managed_runtime,
        };
        session.handshake().await?;
        Ok(session)
    }

    /// Run the session loop in a background thread. If the returned handle is dropped,
    /// the filesystem is unmounted and the given session ends.
    pub fn spawn(self) -> io::Result<BackgroundSession> {
        let sender = self.ch.sender();
        // Take the fuse_session, so that we can unmount it
        let mount = std::mem::take(&mut *self.mount.mount.lock());
        // Spawn the session loop in a background thread.
        let handle = self.handle.clone();
        let guard = handle.spawn(self.run_internal());
        Ok(BackgroundSession {
            guard: Some(guard),
            sender,
            mount,
        })
    }

    /// Internal method for running the session loop. This may not be called directly, since it does not create a temporary runtime to run blocking code.
    async fn run_internal(self) -> io::Result<()> {
        let Session {
            filesystem,
            ch: primary_channel,
            mount: _do_not_umount_yet,
            session_owner,
            proto_version: _,
            config,
            cancellation_token,
            handle,
            managed_runtime: _managed_runtime,
        } = self;

        let handler_task_tracker = TaskTracker::new();
        let mut event_loop_tasks = JoinSet::new();
        let mut filesystem = Arc::new(filesystem);

        let n_event_loop_workers = config.runtime_strategy.event_loop_workers().get();
        let mut channels = Vec::with_capacity(n_event_loop_workers);
        for _ in 0..n_event_loop_workers - 1 {
            if config.clone_fd {
                #[cfg(target_os = "linux")]
                {
                    channels.push(primary_channel.clone_fd().await?);
                    continue;
                }
                #[cfg(not(target_os = "linux"))]
                {
                    return Err(io::Error::other("clone_fd is only supported on Linux"));
                }
            } else {
                channels.push(primary_channel.clone());
            }
        }
        channels.push(primary_channel);

        let cancel_manager = Arc::new(CancelManager::new());

        for (i, ch) in channels.into_iter().enumerate() {
            let thread_name = format!("fuser-{i}");
            let event_loop = SessionEventLoop {
                thread_name: thread_name.clone(),
                filesystem: filesystem.clone(),
                ch,
                allowed: config.acl,
                session_owner,
                cancellation_token: cancellation_token.child_token(),
                handler_runtime: handle.clone(),
                task_tracker: handler_task_tracker.clone(),
                cancel_manager: cancel_manager.clone(),
            };
            event_loop_tasks
                .build_task()
                .name("async_fuser::event_loop")
                .spawn_on(async move { event_loop.event_loop().await }, &handle)?;
        }

        // Wait until all event loop tasks are completed.
        let mut reply: io::Result<()> = Ok(());
        while let Some(result) = event_loop_tasks.join_next().await {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    reply = Err(e);
                }
                Err(_join_error) => {
                    reply = Err(io::Error::other("event loop thread panicked"));
                    break;
                }
            }
        }

        // Wait until all tasks spawned by the event loop and handler runtime are completed.
        handler_task_tracker.close();
        handler_task_tracker.wait().await;

        // Destroy the filesystem.
        let Some(filesystem) = Arc::get_mut(&mut filesystem) else {
            return Err(io::Error::other(
                "BUG: must have one refcount for filesystem",
            ));
        };
        filesystem.destroy();

        // Bootstrap runtime can be destroyed here, and the task will not be canceled because there is no longer an await point.
        reply
    }

    /// Run the session loop that receives kernel requests and dispatches them to method calls into the filesystem.
    ///
    /// Since the method may synchronously block waiting for outstanding event loop and handler tasks to complete, a temporary multithreaded runtime is created exclusively to spawn a task for running this method (i.e. an independent thread but awaitable).
    ///
    /// # Errors
    /// Returns any final error when the session comes to an end.
    pub async fn run(self) -> io::Result<()> {
        self.spawn()?.join().await
    }

    async fn handshake(&mut self) -> io::Result<()> {
        let mut buf = FuseReadBuf::new();
        let buf = buf.as_mut();

        loop {
            let size = match self.ch.receive(buf, &self.cancellation_token).await {
                Ok(size) => size,
                Err(err) if err.raw_os_error() == Some(ENODEV) => {
                    return Err(io::Error::new(
                        io::ErrorKind::NotConnected,
                        "FUSE device disconnected during handshake",
                    ));
                }
                Err(err) => return Err(err),
            };
            // Parse the request
            let request = match ll::AnyRequest::try_from(&buf[..size]) {
                Ok(request) => request,
                Err(err) => {
                    error!("{err}");
                    return Err(io::Error::new(io::ErrorKind::InvalidData, err.to_string()));
                }
            };
            // Extract the init operation
            let op = match request.operation() {
                Ok(op) => op,
                Err(_) => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Failed to parse FUSE operation",
                    ));
                }
            };

            let init = match op {
                ll::Operation::Init(init) => init,
                _ => {
                    error!("Received non-init FUSE operation before init: {}", request);
                    // Send error response and return error - non-init during handshake is invalid
                    let cookie = CancelCookie::dummy(request.unique());
                    <ReplyRaw as Reply>::new(
                        request.unique(),
                        ReplySender::Channel(self.ch.sender()),
                        cookie,
                    )
                    .send_ll(&ResponseErrno(ll::Errno::EIO))
                    .await;
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Received non-init FUSE operation during handshake",
                    ));
                }
            };

            let v = init.version();
            if v.0 > abi::FUSE_KERNEL_VERSION {
                // Kernel has a newer major version than we support.
                // Send our version and wait for a second INIT request with a compatible version.
                debug!(
                    "INIT: Kernel version {} > our version {}, sending our version and waiting for next init",
                    v.0,
                    abi::FUSE_KERNEL_VERSION
                );
                let response = init.reply_version_only();
                let cookie = CancelCookie::dummy(request.unique());
                <ReplyRaw as Reply>::new(
                    request.unique(),
                    ReplySender::Channel(self.ch.sender()),
                    cookie,
                )
                .send_ll(&response)
                .await;
                continue;
            }

            // We don't support ABI versions before 7.6
            if v < Version(7, 6) {
                error!("Unsupported FUSE ABI version {v}");
                let cookie = CancelCookie::dummy(request.unique());
                <ReplyRaw as Reply>::new(
                    request.unique(),
                    ReplySender::Channel(self.ch.sender()),
                    cookie,
                )
                .send_ll(&ResponseErrno(ll::Errno::EPROTO))
                .await;
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    format!("Unsupported FUSE ABI version {v}"),
                ));
            }

            let mut config = KernelConfig::new(init.capabilities(), init.max_readahead(), v);

            // Call filesystem init method and give it a chance to return an error
            let Some(filesystem) = &mut self.filesystem.fs else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "Bug: filesystem must be initialized during handshake",
                ));
            };
            let res = filesystem
                .init(Request::ref_cast(request.header()), &mut config)
                .await;
            if let Err(error) = res {
                let errno = Errno::from_i32(error.raw_os_error().unwrap_or(0));
                let cookie = CancelCookie::dummy(request.unique());
                <ReplyRaw as Reply>::new(
                    request.unique(),
                    ReplySender::Channel(self.ch.sender()),
                    cookie,
                )
                .send_ll(&ResponseErrno(errno))
                .await;
                return Err(error);
            }

            // Remember the ABI version supported by kernel and mark the session initialized.
            self.proto_version = Some(v);

            // Log capability status for debugging
            for bit in 0..64 {
                let bitflags = InitFlags::from_bits_retain(1 << bit);
                if bitflags == InitFlags::FUSE_INIT_EXT {
                    continue;
                }
                let bitflag_is_known = InitFlags::all().contains(bitflags);
                let kernel_supports = init.capabilities().contains(bitflags);
                let we_requested = config.requested.contains(bitflags);
                // On macOS, there's a clash between linux and macOS constants,
                // so we pick macOS ones (last).
                let name = if let Some((name, _)) = bitflags.iter_names().last() {
                    Cow::Borrowed(name)
                } else {
                    Cow::Owned(format!("(1 << {bit})"))
                };
                if we_requested && kernel_supports {
                    debug!("capability {name} enabled")
                } else if we_requested {
                    debug!("capability {name} not supported by kernel")
                } else if kernel_supports {
                    debug!("capability {name} not requested by client")
                } else if bitflag_is_known {
                    debug!("capability {name} not supported nor requested")
                }
            }

            // Reply with our desired version and settings.
            debug!(
                "INIT response: ABI {}.{}, flags {:#x}, max readahead {}, max write {}",
                abi::FUSE_KERNEL_VERSION,
                abi::FUSE_KERNEL_MINOR_VERSION,
                init.capabilities() & config.requested,
                config.max_readahead,
                config.max_write
            );

            let response = init.reply(&config);
            let cookie = CancelCookie::dummy(request.unique());
            <ReplyRaw as Reply>::new(
                request.unique(),
                ReplySender::Channel(self.ch.sender()),
                cookie,
            )
            .send_ll(&response)
            .await;

            return Ok(());
        }
    }

    /// Unmount the filesystem
    pub async fn unmount(&mut self, flags: &[UnmountOption]) -> io::Result<()> {
        self.mount.umount(flags).await
    }

    /// Returns a thread-safe object that can be used to unmount the Filesystem
    pub fn unmount_callable(&mut self) -> SessionUnmounter {
        SessionUnmounter {
            mount: self.mount.mount.clone(),
        }
    }

    /// Returns an object that can be used to send notifications to the kernel
    pub fn notifier(&self) -> Notifier {
        Notifier::new(self.ch.sender())
    }
}

/// A thread-safe object that can be used to unmount a Filesystem
pub struct SessionUnmounter {
    mount: Arc<Mutex<Option<Mount>>>,
}

impl Debug for SessionUnmounter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionUnmounter")
            .field("mount", &*self.mount.lock())
            .finish()
    }
}

impl SessionUnmounter {
    /// Unmount the filesystem
    pub async fn unmount(&mut self, flags: &[UnmountOption]) -> io::Result<()> {
        let mut guard = self.mount.lock();
        let mount = match guard.take() {
            Some(mount) => mount,
            None => return Ok(()),
        };
        mount.umount(flags).await.map_err(|(mount, error)| {
            *guard = mount;
            error
        })?;
        Ok(())
    }
}

pub(crate) struct SessionEventLoop<FS: Filesystem> {
    /// Cache thread name for faster `debug!`.
    pub(crate) thread_name: String,
    pub(crate) ch: Channel,
    pub(crate) filesystem: Arc<FilesystemHolder<FS>>,
    pub(crate) cancel_manager: Arc<CancelManager>,
    pub(crate) allowed: SessionACL,
    pub(crate) session_owner: Uid,
    pub(crate) cancellation_token: CancellationToken,
    pub(crate) handler_runtime: Handle,
    pub(crate) task_tracker: TaskTracker,
}

impl<FS: Filesystem> SessionEventLoop<FS> {
    async fn event_loop(&self) -> io::Result<()> {
        // Buffer for receiving requests from the kernel. Only one is allocated and
        // it is reused immediately after dispatching to conserve memory and allocations.
        let mut buf = FuseReadBuf::new();
        let buf = buf.as_mut();
        loop {
            let size = match self.ch.receive(buf, &self.cancellation_token).await {
                Ok(size) => size,
                // If the cancellation token is triggered, return Ok(()) immediately. FS destruction occurs later
                Err(err) if err.raw_os_error() == Some(Errno::ECANCELED.into()) => {
                    return Ok(());
                }
                Err(err) if err.raw_os_error() == Some(ENODEV) => return Ok(()),
                Err(err) => return Err(err),
            };
            match RequestWithSender::new(self.ch.sender(), &buf[..size]) {
                // Dispatch request
                Some(req) => {
                    let cookie = CancelCookie::dummy(req.request.unique());
                    if let Ok(Operation::Destroy(_)) = req.request.operation() {
                        req.reply::<ReplyEmpty>(cookie).ok().await;
                        return Ok(());
                    } else {
                        req.dispatch(self).await;
                    }
                }
                // Quit loop on illegal request
                None => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Invalid request",
                    ));
                }
            }
        }
    }
}

/// The background session data structure
#[derive(Debug)]
pub struct BackgroundSession {
    /// Thread guard of the background session
    pub guard: Option<JoinHandle<io::Result<()>>>,
    /// Object for creating Notifiers for client use
    sender: ChannelSender,
    /// Ensures the filesystem is unmounted when the session ends
    mount: Option<Mount>,
}

impl BackgroundSession {
    async fn _umount_and_join(&mut self, flags: &[UnmountOption]) -> Result<(), io::Error> {
        if let Some(mount) = self.mount.take() {
            match mount.umount(flags).await {
                Ok(()) => {}
                Err((mount, error)) => {
                    self.mount = mount;
                    return Err(error);
                }
            }
        }
        if let Some(guard) = self.guard.take() {
            guard.await.map_err(io::Error::other)??
        }
        Ok(())
    }

    /// Unmount the filesystem and join the background thread.
    pub async fn umount_and_join(
        mut self,
        flags: &[UnmountOption],
    ) -> Result<(), (Option<Self>, io::Error)> {
        self._umount_and_join(flags).await.map_err(|e| {
            (
                if self.mount.is_some() {
                    Some(self)
                } else {
                    None
                },
                e,
            )
        })
    }

    /// Unmount with the detached flag, and retrieve the inner join handle so that cleanup tasks may wait on it.
    #[cfg(target_os = "linux")]
    pub async fn umount_and_detach(mut self) -> Result<JoinHandle<io::Result<()>>, io::Error> {
        if let Some(mount) = self.mount.take() {
            match mount.umount(&[UnmountOption::Detach]).await {
                Ok(()) => {}
                Err((mount, error)) => {
                    self.mount = mount;
                    return Err(error);
                }
            }
        }
        self.guard
            .take()
            .ok_or_else(|| io::Error::other("FUSE session guard not found"))
    }

    /// Returns an object that can be used to send notifications to the kernel
    pub fn notifier(&self) -> Notifier {
        Notifier::new(self.sender.clone())
    }

    /// Join the filesystem thread without unmounting.
    pub async fn join(mut self) -> io::Result<()> {
        if let Some(guard) = self.guard.take() {
            guard.await.map_err(io::Error::other)?
        } else {
            Ok(())
        }
    }
}

impl Drop for BackgroundSession {
    fn drop(&mut self) {
        runtime::execute_future_from_sync(async move {
            let _ = self
                ._umount_and_join(drop_umount_flags())
                .await
                .inspect_err(|e| {
                    log::error!("Error unmounting filesystem: {e}");
                });
        });
    }
}

#[cfg(test)]
mod tests {
    use crate::{Config, Filesystem, Session, session::RuntimeStrategy};

    struct DummyFS {}

    impl Filesystem for DummyFS {}

    #[tokio::test]
    #[test_log::test]
    async fn test_session_lifecycle() {
        // Create a temporary directory to mount the filesystem to
        let temp_dir = tempdir::TempDir::new("test_session_lifecycle").unwrap();
        let mount_point = temp_dir.path();

        // Create a session
        let session = Session::new(DummyFS {}, mount_point, &Config::default())
            .await
            .unwrap();

        // Spawn the background session
        let bg_session = session.spawn().unwrap();

        // Unmount the session
        bg_session.umount_and_join(&[]).await.unwrap();
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_session_lifecycle_with_sync_drop() {
        // Create a temporary directory to mount the filesystem to
        let temp_dir = tempdir::TempDir::new("test_session_lifecycle").unwrap();
        let mount_point = temp_dir.path();

        // Create a session
        let session = Session::new(DummyFS {}, mount_point, &Config::default())
            .await
            .unwrap();

        // Spawn the background session
        let bg_session = session.spawn().unwrap();

        // Unmount the session using sync mode in a current thread runtime to simulate the function being called from a Drop impl, should not deadlock with internal tasks
        drop(bg_session);
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_runtime_strategy_in_current_thread_context() {
        let strategy = RuntimeStrategy::default();
        assert!(strategy.verify().is_ok());
        assert!(matches!(
            strategy,
            RuntimeStrategy::Managed {
                n_event_loop_workers: None,
                n_handler_workers,
            } if n_handler_workers == std::thread::available_parallelism().ok(),
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[test_log::test]
    async fn test_runtime_strategy_in_multi_thread_context() {
        let strategy = RuntimeStrategy::default();
        assert!(strategy.verify().is_ok());
        assert!(matches!(
            strategy,
            RuntimeStrategy::Unmanaged {
                n_event_loop_workers: None,
                handle,
            } if handle.metrics().num_workers() == 4
        ));
    }
}
