use crate::{dev_fuse::DevFuse, passthrough::BackingId};
use std::{
    io,
    os::fd::{AsFd, AsRawFd, BorrowedFd, RawFd},
    sync::Arc,
};

#[derive(Debug, Clone)]
pub(crate) struct Channel(Arc<DevFuse>);

impl AsFd for Channel {
    fn as_fd(&self) -> BorrowedFd<'_> {
        <Arc<DevFuse> as AsFd>::as_fd(&self.0)
    }
}

impl AsRawFd for Channel {
    fn as_raw_fd(&self) -> RawFd {
        self.0.as_raw_fd()
    }
}

impl Channel {
    /// Initialize a new communication channel to the kernel driver using an existing
    /// `/dev/fuse` file descriptor. The kernel driver will delegate filesystem operations
    /// to this channel.
    pub(crate) fn new(device: Arc<DevFuse>) -> Self {
        Self(device)
    }

    /// Receives data up to the capacity of the buffer.
    async fn receive(&self, buffer: &mut [u8]) -> io::Result<usize> {
        let mut upgraded_target = unsafe { self.0.client.to_target(self.0.file.as_ref()) };
        self.0.client.read(&mut upgraded_target, buffer).await
    }

    /// Receives data up to the capacity of the given buffer, retrying on errors that are safe to retry
    /// ([`ENOENT`](libc::ENOENT), [`EAGAIN`](libc::EAGAIN), [`EINTR`](libc::EINTR)).
    pub(crate) async fn receive_retrying(&self, buffer: &mut [u8]) -> io::Result<usize> {
        loop {
            match self.receive(buffer).await {
                Ok(n) => return Ok(n),
                Err(e) => {
                    let raw_os_error = e.raw_os_error();
                    match raw_os_error {
                        Some(libc::ENOENT) | Some(libc::EAGAIN) | Some(libc::EINTR) => continue,
                        _ => return Err(e),
                    }
                }
            }
        }
    }

    /// Returns a sender object for this channel. The sender object can be
    /// used to send to the channel. Multiple sender objects can be used
    /// and they can safely be sent to other threads.
    pub(crate) fn sender(&self) -> ChannelSender {
        // Since write/writev syscalls are threadsafe, we can simply create
        // a sender by using the same file and use it in other threads.
        ChannelSender(self.0.clone())
    }

    /// Clone the FUSE device fd using FUSE_DEV_IOC_CLONE ioctl.
    ///
    /// This creates a new fd that can read FUSE requests independently,
    /// enabling true parallel request processing. The kernel distributes
    /// requests across all cloned fds.
    ///
    /// Requires Linux 4.5+. Returns an error on older kernels or non-Linux.
    #[cfg(target_os = "linux")]
    pub(crate) async fn clone_fd(&self) -> io::Result<Channel> {
        let devfuse = self.0.clone_fd().await?;
        Ok(Channel::new(Arc::new(devfuse)))
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ChannelSender(Arc<DevFuse>);

impl AsFd for ChannelSender {
    fn as_fd(&self) -> BorrowedFd<'_> {
        <Arc<DevFuse> as AsFd>::as_fd(&self.0)
    }
}

impl AsRawFd for ChannelSender {
    fn as_raw_fd(&self) -> RawFd {
        self.0.as_raw_fd()
    }
}

impl ChannelSender {
    pub(crate) async fn send(&self, buffers: &[io::IoSlice<'_>]) -> io::Result<()> {
        let mut upgraded_target = unsafe { self.0.client.to_target(self.0.file.as_ref()) };
        // SAFETY: parallel write to /dev/fuse is supported, and the target is not changed in the call.
        let rc = self
            .0
            .client
            .write_vectored(&mut upgraded_target, buffers)
            .await?;
        // writev is atomic, so do not need to check how many bytes are written.
        // libfuse does not do it either
        // https://github.com/libfuse/libfuse/blob/6278995cca991978abd25ebb2c20ebd3fc9e8a13/lib/fuse_lowlevel.c#L267
        debug_assert_eq!(buffers.iter().map(|b| b.len()).sum::<usize>(), rc);
        Ok(())
    }

    pub(crate) async fn open_backing(&self, fd: BorrowedFd<'_>) -> io::Result<BackingId> {
        BackingId::create(&self.0, fd).await
    }

    pub(crate) unsafe fn wrap_backing(&self, id: u32) -> BackingId {
        unsafe { BackingId::wrap_raw(&self.0, id) }
    }
}
