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
        write!(f, "DevFuse(fd={})", self.file.as_fd().as_raw_fd())
    }
}

impl AsRawFd for DevFuse {
    fn as_raw_fd(&self) -> RawFd {
        self.file.as_fd().as_raw_fd()
    }
}

impl AsFd for DevFuse {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.file.as_fd()
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
        let target_raw_fd = fuse.as_raw_fd();
        let mut source_fd = self.file.as_fd().as_raw_fd() as u32;
        tokio::task::spawn_blocking(move || -> io::Result<()> {
            unsafe { crate::ll::ioctl::fuse_dev_ioc_clone(target_raw_fd, &mut source_fd)? };
            Ok(())
        })
        .await
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))??;
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
