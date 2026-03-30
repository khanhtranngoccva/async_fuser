use async_hybrid_fs::{Client, Permissions, UringCfg, UringTarget};
use nix::fcntl::OFlag;
use std::{
    io,
    ops::{Deref, DerefMut},
    os::fd::{AsFd, AsRawFd, BorrowedFd, OwnedFd, RawFd},
    sync::Arc,
};

/// A newtype for [`File`] that represents the `/dev/fuse` device.
pub(crate) struct DevFuse {
    pub(crate) client: Arc<Client>,
    // Use UringTarget to support both fixed targets and dynamic targets
    pub(crate) file: Box<dyn UringTarget + Send + Sync>,
}

impl std::fmt::Debug for DevFuse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "DevFuse(fd={})",
            self.file.as_file_descriptor().as_raw_fd()
        )
    }
}

impl AsRawFd for DevFuse {
    fn as_raw_fd(&self) -> RawFd {
        self.file.as_file_descriptor().as_raw_fd()
    }
}

impl AsFd for DevFuse {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.file.as_file_descriptor()
    }
}

impl Deref for DevFuse {
    type Target = dyn UringTarget + Send + Sync;

    fn deref(&self) -> &Self::Target {
        self.file.as_ref()
    }
}

impl DerefMut for DevFuse {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.file.as_mut()
    }
}

impl DevFuse {
    pub(crate) const PATH: &'static str = "/dev/fuse";

    #[allow(dead_code)]
    pub(crate) async fn open() -> io::Result<Self> {
        // TODO: make this configurable
        let client = Arc::new(
            Client::build(UringCfg::default())
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?,
        );
        let fuse = client
            .open_path(Self::PATH, OFlag::O_RDWR, Permissions::from_mode(0))
            .await?;
        let registered = client
            .register_owned(fuse)
            .map::<Box<dyn UringTarget + Send + Sync>, _>(|f| Box::new(f))
            .unwrap_or_else(|e| Box::new(e.1));
        Ok(Self {
            client,
            file: registered,
        })
    }

    #[allow(dead_code)]
    pub(crate) async fn try_from_fd(fd: OwnedFd) -> io::Result<Self> {
        let client = Arc::new(
            Client::build(UringCfg::default())
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?,
        );
        let file = client
            .register_owned(fd)
            .map::<Box<dyn UringTarget + Send + Sync>, _>(|f| Box::new(f))
            .unwrap_or_else(|e| Box::new(e.1));
        Ok(Self { client, file })
    }

    #[allow(dead_code)]
    #[cfg(target_os = "linux")]
    pub(crate) async fn clone_fd(&self) -> io::Result<Self> {
        let client = self.client.clone();
        let fuse = client
            .open_path(Self::PATH, OFlag::O_RDWR, Permissions::from_mode(0))
            .await?;
        // SAFETY: fuse_dev_ioc_clone is a valid ioctl for /dev/fuse
        let target_fd_borrowed = fuse.as_fd();
        let source_fd_borrowed = self.file.as_file_descriptor();
        let (_, mut futures) = unsafe {
            async_scoped::TokioScope::scope_and_collect(|scope| {
                scope.spawn_blocking(move || -> io::Result<()> {
                    let mut source_fd_raw = source_fd_borrowed.as_raw_fd() as u32;
                    crate::ll::ioctl::fuse_dev_ioc_clone(
                        target_fd_borrowed.as_raw_fd(),
                        &mut source_fd_raw,
                    )?;
                    Ok(())
                });
            })
        }
        .await;
        let future = futures.pop().expect("no future returned");
        future.expect("failed to join future")?;
        let registered = client
            .register_owned(fuse)
            .map::<Box<dyn UringTarget + Send + Sync>, _>(|f| Box::new(f))
            .unwrap_or_else(|e| Box::new(e.1));
        Ok(Self {
            client,
            file: registered,
        })
    }
}
