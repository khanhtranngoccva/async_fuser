use std::{
    io,
    ops::{Deref, DerefMut},
    os::fd::{AsFd, AsRawFd, BorrowedFd, RawFd},
};
use tokio::fs::{File, OpenOptions};

/// A newtype for [`File`] that represents the `/dev/fuse` device.
#[derive(Debug)]
pub(crate) struct DevFuse(pub(crate) File);

impl AsRawFd for DevFuse {
    fn as_raw_fd(&self) -> RawFd {
        self.0.as_raw_fd()
    }
}

impl AsFd for DevFuse {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.0.as_fd()
    }
}

impl Deref for DevFuse {
    type Target = File;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for DevFuse {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl DevFuse {
    pub(crate) const PATH: &'static str = "/dev/fuse";

    #[allow(dead_code)] // Not used with every feature.
    pub(crate) async fn open() -> io::Result<Self> {
        OpenOptions::new()
            .read(true)
            .write(true)
            .open(Self::PATH)
            .await
            .map(Self)
    }
}
