#[allow(unused)]
use std::convert::TryInto;
#[allow(unused)]
use std::ffi::OsStr;
use std::io;

use crate::INodeNo;
use crate::channel::ChannelSender;
use crate::ll::fuse_abi::fuse_notify_code as notify_code;
use crate::ll::notify::Notification;

/// A handle to a pending `poll()` request.
#[derive(Copy, Clone, Debug)]
pub struct PollHandle(pub u64);

/// A [handle](PollHandle) to a pending `poll()` request coupled with notifier reference.
/// Can be saved and used to notify the kernel when a poll is ready.
#[derive(Clone)]
pub struct PollNotifier {
    handle: PollHandle,
    notifier: Notifier,
}

impl PollNotifier {
    pub(crate) fn new(cs: ChannelSender, kh: PollHandle) -> Self {
        Self {
            handle: kh,
            notifier: Notifier::new(cs),
        }
    }

    /// Handle associated with this poll notifier.
    pub fn handle(&self) -> PollHandle {
        self.handle
    }

    /// Notify the kernel that the associated file handle is ready to be polled.
    /// # Errors
    /// Returns an error if the kernel rejects the notification.
    pub async fn notify(self) -> io::Result<()> {
        self.notifier.poll(self.handle).await
    }
}

impl std::fmt::Debug for PollNotifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("PollHandle").field(&self.handle).finish()
    }
}

/// A handle by which the application can send notifications to the server
#[derive(Debug, Clone)]
pub struct Notifier(ChannelSender);

impl Notifier {
    pub(crate) fn new(cs: ChannelSender) -> Self {
        Self(cs)
    }

    /// Notify poll clients of I/O readiness
    /// # Errors
    /// Returns an error if the kernel rejects the notification.
    pub async fn poll(&self, kh: PollHandle) -> io::Result<()> {
        let notif = Notification::new_poll(kh);
        self.send(notify_code::FUSE_POLL, &notif).await
    }

    /// Invalidate the kernel cache for a given directory entry
    /// # Errors
    /// Returns an error if the notification data is too large.
    /// Returns an error if the kernel rejects the notification.
    pub async fn inval_entry(&self, parent: INodeNo, name: &OsStr) -> io::Result<()> {
        let notif = Notification::new_inval_entry(parent, name).map_err(Self::too_big_err)?;
        self.send_inval(notify_code::FUSE_NOTIFY_INVAL_ENTRY, &notif)
            .await
    }

    /// Invalidate the kernel cache for a given inode (metadata and
    /// data in the given range)
    /// # Errors
    /// Returns an error if the kernel rejects the notification.
    pub async fn inval_inode(&self, ino: INodeNo, offset: i64, len: i64) -> io::Result<()> {
        let notif = Notification::new_inval_inode(ino, offset, len);
        self.send_inval(notify_code::FUSE_NOTIFY_INVAL_INODE, &notif)
            .await
    }

    /// Update the kernel's cached copy of a given inode's data
    /// # Errors
    /// Returns an error if the notification data is too large.
    /// Returns an error if the kernel rejects the notification.
    pub async fn store(&self, ino: INodeNo, offset: u64, data: &[u8]) -> io::Result<()> {
        let notif = Notification::new_store(ino, offset, data).map_err(Self::too_big_err)?;
        // Not strictly an invalidate, but the inode we're operating
        // on may have been evicted anyway, so treat is as such
        self.send_inval(notify_code::FUSE_NOTIFY_STORE, &notif)
            .await
    }

    /// Invalidate the kernel cache for a given directory entry and inform
    /// inotify watchers of a file deletion.
    /// # Errors
    /// Returns an error if the notification data is too large.
    /// Returns an error if the kernel rejects the notification.
    pub async fn delete(&self, parent: INodeNo, child: INodeNo, name: &OsStr) -> io::Result<()> {
        let notif = Notification::new_delete(parent, child, name).map_err(Self::too_big_err)?;
        self.send_inval(notify_code::FUSE_NOTIFY_DELETE, &notif)
            .await
    }

    #[allow(unused)]
    async fn send_inval(
        &self,
        code: notify_code,
        notification: &Notification<'_>,
    ) -> io::Result<()> {
        match self.send(code, notification).await {
            // ENOENT is harmless for an invalidation (the
            // kernel may have already dropped the cached
            // entry on its own anyway), so ignore it.
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            x => x,
        }
    }

    async fn send(&self, code: notify_code, notification: &Notification<'_>) -> io::Result<()> {
        let future = notification.with_iovec(code, async |iov| self.0.send(iov).await);
        future.await.map_err(Self::too_big_err)?
    }

    /// Create an error for indicating when a notification message
    /// would exceed the capacity that its length descriptor field is
    /// capable of encoding.
    fn too_big_err(tfie: std::num::TryFromIntError) -> io::Error {
        io::Error::new(io::ErrorKind::Other, format!("Data too large: {tfie:?}"))
    }
}
