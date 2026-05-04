//! Filesystem operation request
//!
//! A request represents information about a filesystem operation the kernel driver wants us to
//! perform.
//!
//! TODO: This module is meant to go away soon in favor of `ll::Request`.

use std::convert::TryFrom;

use log::debug;
use log::error;

use crate::Filesystem;
use crate::PollNotifier;
use crate::RenameFlags;
use crate::Request;
use crate::channel::ChannelSender;
use crate::forget_one::ForgetOne;
use crate::ll;
use crate::ll::Errno;
use crate::ll::ResponseData;
use crate::ll::ResponseErrno;
use crate::reply::Reply;
use crate::reply::ReplyDirectory;
use crate::reply::ReplyDirectoryPlus;
use crate::reply::ReplyRaw;
use crate::reply::ReplySender;
use crate::session::SessionACL;
use crate::session::SessionEventLoop;

/// Request data structure
#[derive(Debug)]
pub(crate) struct RequestWithSender<'a> {
    /// Channel sender for sending the reply
    ch: ChannelSender,
    /// Parsed request
    pub(crate) request: ll::AnyRequest<'a>,
}

impl<'a> RequestWithSender<'a> {
    /// Create a new request from the given data
    pub(crate) fn new(ch: ChannelSender, data: &'a [u8]) -> Option<RequestWithSender<'a>> {
        let request = match ll::AnyRequest::try_from(data) {
            Ok(request) => request,
            Err(err) => {
                error!("{err}");
                return None;
            }
        };

        Some(Self { ch, request })
    }

    /// Dispatch request to the given filesystem.
    /// This calls the appropriate filesystem operation method for the
    /// request and sends back the returned reply to the kernel
    pub(crate) async fn dispatch<FS: Filesystem>(&self, se: &SessionEventLoop<FS>) {
        debug!("{} thread={}", self.request, se.thread_name);
        match self.dispatch_req(se).await {
            Ok(Some(resp)) => self.reply::<ReplyRaw>().send_ll(&resp).await,
            Ok(None) => {}
            Err(errno) => {
                self.reply::<ReplyRaw>()
                    .send_ll(&ResponseErrno(errno))
                    .await
            }
        }
    }

    async fn dispatch_req<FS: Filesystem>(
        &self,
        se: &SessionEventLoop<FS>,
    ) -> Result<Option<ResponseData>, Errno> {
        let op = self.request.operation().map_err(|_| Errno::ENOSYS)?;
        // Implement allow_root & access check for auto_unmount
        if (se.allowed == SessionACL::RootAndOwner
            && self.request.uid() != se.session_owner
            && !self.request.uid().is_root())
            || (se.allowed == SessionACL::Owner && self.request.uid() != se.session_owner)
        {
            {
                match op {
                    // Only allow operations that the kernel may issue without a uid set
                    ll::Operation::Init(_)
                    | ll::Operation::Destroy(_)
                    | ll::Operation::Read(_)
                    | ll::Operation::ReadDir(_)
                    | ll::Operation::BatchForget(_)
                    | ll::Operation::Forget(_)
                    | ll::Operation::Write(_)
                    | ll::Operation::FSync(_)
                    | ll::Operation::FSyncDir(_)
                    | ll::Operation::Release(_)
                    | ll::Operation::ReleaseDir(_) => {}
                    ll::Operation::ReadDirPlus(_) => {}
                    _ => {
                        return Err(Errno::EACCES);
                    }
                }
            }
        }

        let header = self.request_header().clone();
        let nodeid = self.request.nodeid();
        let filesystem = se.filesystem.clone();

        match op {
            // Filesystem initialization - should not happen after handshake completed
            ll::Operation::Init(_) => {
                error!("Unexpected FUSE_INIT after handshake completed");
                return Err(Errno::EIO);
            }
            ll::Operation::Destroy(_x) => {
                // This is handled before dispatch call.
                return Err(Errno::EIO);
            }
            ll::Operation::Interrupt(_) => {
                // TODO: handle FUSE_INTERRUPT
                return Err(Errno::ENOSYS);
            }
            ll::Operation::Lookup(x) => {
                let name = x.name().as_os_str().to_os_string();
                let reply = self.reply();
                se.handler_runtime
                    .spawn(se.task_tracker.track_future(async move {
                        filesystem.lookup(&header, nodeid, &name, reply).await;
                    }));
            }
            ll::Operation::Forget(x) => {
                let nlookup = x.nlookup();
                se.handler_runtime
                    .spawn(se.task_tracker.track_future(async move {
                        filesystem.forget(&header, nodeid, nlookup).await;
                    }));
            }
            ll::Operation::GetAttr(_attr) => {
                let fh = _attr.file_handle();
                let reply = self.reply();
                se.handler_runtime
                    .spawn(se.task_tracker.track_future(async move {
                        filesystem.getattr(&header, nodeid, fh, reply).await;
                    }));
            }
            ll::Operation::SetAttr(x) => {
                let mode = x.mode();
                let uid = x.uid();
                let gid = x.gid();
                let size = x.size();
                let atime = x.atime();
                let mtime = x.mtime();
                let ctime = x.ctime();
                let fh = x.file_handle();
                let crtime = x.crtime();
                let chgtime = x.chgtime();
                let bkuptime = x.bkuptime();
                let flags = x.flags();
                let reply = self.reply();
                se.handler_runtime
                    .spawn(se.task_tracker.track_future(async move {
                        filesystem
                            .setattr(
                                &header, nodeid, mode, uid, gid, size, atime, mtime, ctime, fh,
                                crtime, chgtime, bkuptime, flags, reply,
                            )
                            .await;
                    }));
            }
            ll::Operation::ReadLink(_) => {
                let reply = self.reply();
                se.handler_runtime
                    .spawn(se.task_tracker.track_future(async move {
                        filesystem.readlink(&header, nodeid, reply).await;
                    }));
            }
            ll::Operation::MkNod(x) => {
                let name = x.name().as_os_str().to_os_string();
                let mode = x.mode();
                let umask = x.umask();
                let rdev = x.rdev();
                let reply = self.reply();
                se.handler_runtime
                    .spawn(se.task_tracker.track_future(async move {
                        filesystem
                            .mknod(&header, nodeid, &name, mode, umask, rdev, reply)
                            .await;
                    }));
            }
            ll::Operation::MkDir(x) => {
                let name = x.name().as_os_str().to_os_string();
                let mode = x.mode();
                let umask = x.umask();
                let reply = self.reply();
                se.handler_runtime
                    .spawn(se.task_tracker.track_future(async move {
                        filesystem
                            .mkdir(&header, nodeid, &name, mode, umask, reply)
                            .await;
                    }));
            }
            ll::Operation::Unlink(x) => {
                let name = x.name().as_os_str().to_os_string();
                let reply = self.reply();
                se.handler_runtime
                    .spawn(se.task_tracker.track_future(async move {
                        filesystem.unlink(&header, nodeid, &name, reply).await;
                    }));
            }
            ll::Operation::RmDir(x) => {
                let name = x.name().as_os_str().to_os_string();
                let reply = self.reply();
                se.handler_runtime
                    .spawn(se.task_tracker.track_future(async move {
                        filesystem.rmdir(&header, nodeid, &name, reply).await;
                    }));
            }
            ll::Operation::SymLink(x) => {
                let link_name = x.link_name().as_os_str().to_os_string();
                let target = x.target().to_owned();
                let reply = self.reply();
                se.handler_runtime
                    .spawn(se.task_tracker.track_future(async move {
                        filesystem
                            .symlink(&header, nodeid, &link_name, &target, reply)
                            .await;
                    }));
            }
            ll::Operation::Rename(x) => {
                let src_name = x.src().name.as_os_str().to_os_string();
                let dest_dir = x.dest().dir;
                let dest_name = x.dest().name.as_os_str().to_os_string();
                let reply = self.reply();
                se.handler_runtime
                    .spawn(se.task_tracker.track_future(async move {
                        filesystem
                            .rename(
                                &header,
                                nodeid,
                                &src_name,
                                dest_dir,
                                &dest_name,
                                RenameFlags::empty(),
                                reply,
                            )
                            .await;
                    }));
            }
            ll::Operation::Link(x) => {
                let inode_no = x.inode_no();
                let dest_name = x.dest().name.as_os_str().to_os_string();
                let reply = self.reply();
                se.handler_runtime
                    .spawn(se.task_tracker.track_future(async move {
                        filesystem
                            .link(&header, inode_no, nodeid, &dest_name, reply)
                            .await;
                    }));
            }
            ll::Operation::Open(x) => {
                let flags = x.flags();
                let reply = self.reply();
                se.handler_runtime
                    .spawn(se.task_tracker.track_future(async move {
                        filesystem.open(&header, nodeid, flags, reply).await;
                    }));
            }
            ll::Operation::Read(x) => {
                let file_handle = x.file_handle();
                let offset = x.offset()?;
                let size = x.size();
                let flags = x.flags();
                let lock_owner = x.lock_owner();
                let reply = self.reply();
                se.handler_runtime
                    .spawn(se.task_tracker.track_future(async move {
                        filesystem
                            .read(
                                &header,
                                nodeid,
                                file_handle,
                                offset,
                                size,
                                flags,
                                lock_owner,
                                reply,
                            )
                            .await;
                    }));
            }
            ll::Operation::Write(x) => {
                let file_handle = x.file_handle();
                let offset = x.offset()?;
                // TODO: an extra allocate and copy is required to free the event loop buffer for reading subsequent requests, consider using a bucketed buffer pool to eliminate allocations
                let data = x.data().to_vec();
                let write_flags = x.write_flags();
                let flags = x.flags();
                let lock_owner = x.lock_owner();
                let reply = self.reply();
                se.handler_runtime
                    .spawn(se.task_tracker.track_future(async move {
                        filesystem
                            .write(
                                &header,
                                nodeid,
                                file_handle,
                                offset,
                                &data,
                                write_flags,
                                flags,
                                lock_owner,
                                reply,
                            )
                            .await;
                    }));
            }
            ll::Operation::Flush(x) => {
                let file_handle = x.file_handle();
                let lock_owner = x.lock_owner();
                let reply = self.reply();
                se.handler_runtime
                    .spawn(se.task_tracker.track_future(async move {
                        filesystem
                            .flush(&header, nodeid, file_handle, lock_owner, reply)
                            .await;
                    }));
            }
            ll::Operation::Release(x) => {
                let file_handle = x.file_handle();
                let flags = x.flags();
                let lock_owner = x.lock_owner();
                let flush = x.flush();
                let reply = self.reply();
                se.handler_runtime
                    .spawn(se.task_tracker.track_future(async move {
                        filesystem
                            .release(
                                &header,
                                nodeid,
                                file_handle,
                                flags,
                                lock_owner,
                                flush,
                                reply,
                            )
                            .await;
                    }));
            }
            ll::Operation::FSync(x) => {
                let file_handle = x.file_handle();
                let datasync = x.fdatasync();
                let reply = self.reply();
                se.handler_runtime
                    .spawn(se.task_tracker.track_future(async move {
                        filesystem
                            .fsync(&header, nodeid, file_handle, datasync, reply)
                            .await;
                    }));
            }
            ll::Operation::OpenDir(x) => {
                let flags = x.flags();
                let reply = self.reply();
                se.handler_runtime
                    .spawn(se.task_tracker.track_future(async move {
                        filesystem.opendir(&header, nodeid, flags, reply).await;
                    }));
            }
            ll::Operation::ReadDir(x) => {
                let file_handle = x.file_handle();
                let offset = x.offset();
                let reply = ReplyDirectory::new(
                    self.request.unique(),
                    ReplySender::Channel(self.ch.clone()),
                    x.size() as usize,
                );
                se.handler_runtime
                    .spawn(se.task_tracker.track_future(async move {
                        filesystem
                            .readdir(&header, nodeid, file_handle, offset, reply)
                            .await;
                    }));
            }
            ll::Operation::ReleaseDir(x) => {
                let file_handle = x.file_handle();
                let flags = x.flags();
                let reply = self.reply();
                se.handler_runtime
                    .spawn(se.task_tracker.track_future(async move {
                        filesystem
                            .releasedir(&header, nodeid, file_handle, flags, reply)
                            .await;
                    }));
            }
            ll::Operation::FSyncDir(x) => {
                let file_handle = x.file_handle();
                let datasync = x.fdatasync();
                let reply = self.reply();
                se.handler_runtime
                    .spawn(se.task_tracker.track_future(async move {
                        filesystem
                            .fsyncdir(&header, nodeid, file_handle, datasync, reply)
                            .await;
                    }));
            }
            ll::Operation::StatFs(_) => {
                let reply = self.reply();
                se.handler_runtime
                    .spawn(se.task_tracker.track_future(async move {
                        filesystem.statfs(&header, nodeid, reply).await;
                    }));
            }
            ll::Operation::SetXAttr(x) => {
                let name = x.name().to_os_string();
                // TODO: consider using a buffer pool
                let value = x.value().to_vec();
                let flags = x.flags();
                let position = x.position();
                let reply = self.reply();
                se.handler_runtime
                    .spawn(se.task_tracker.track_future(async move {
                        filesystem
                            .setxattr(&header, nodeid, &name, &value, flags, position, reply)
                            .await;
                    }));
            }
            ll::Operation::GetXAttr(x) => {
                let name = x.name().to_os_string();
                let size = x.size_u32();
                let reply = self.reply();
                se.handler_runtime
                    .spawn(se.task_tracker.track_future(async move {
                        filesystem
                            .getxattr(&header, nodeid, &name, size, reply)
                            .await;
                    }));
            }
            ll::Operation::ListXAttr(x) => {
                let size = x.size();
                let reply = self.reply();
                se.handler_runtime
                    .spawn(se.task_tracker.track_future(async move {
                        filesystem.listxattr(&header, nodeid, size, reply).await;
                    }));
            }
            ll::Operation::RemoveXAttr(x) => {
                let name = x.name().to_os_string();
                let reply = self.reply();
                se.handler_runtime
                    .spawn(se.task_tracker.track_future(async move {
                        filesystem.removexattr(&header, nodeid, &name, reply).await;
                    }));
            }
            ll::Operation::Access(x) => {
                let mask = x.mask();
                let reply = self.reply();
                se.handler_runtime
                    .spawn(se.task_tracker.track_future(async move {
                        filesystem.access(&header, nodeid, mask, reply).await;
                    }));
            }
            ll::Operation::Create(x) => {
                let name = x.name().as_os_str().to_os_string();
                let mode = x.mode();
                let umask = x.umask();
                let flags = x.flags();
                let reply = self.reply();
                se.handler_runtime
                    .spawn(se.task_tracker.track_future(async move {
                        filesystem
                            .create(&header, nodeid, &name, mode, umask, flags, reply)
                            .await;
                    }));
            }
            ll::Operation::GetLk(x) => {
                let file_handle = x.file_handle();
                let lock_owner = x.lock_owner();
                let start = x.lock().range.0;
                let end = x.lock().range.1;
                let typ = x.lock().typ;
                let pid = x.lock().pid;
                let reply = self.reply();
                se.handler_runtime
                    .spawn(se.task_tracker.track_future(async move {
                        filesystem
                            .getlk(
                                &header,
                                nodeid,
                                file_handle,
                                lock_owner,
                                start,
                                end,
                                typ,
                                pid,
                                reply,
                            )
                            .await;
                    }));
            }
            ll::Operation::SetLk(x) => {
                let file_handle = x.file_handle();
                let lock_owner = x.lock_owner();
                let start = x.lock().range.0;
                let end = x.lock().range.1;
                let typ = x.lock().typ;
                let pid = x.lock().pid;
                let sleep = x.sleep();
                let reply = self.reply();
                se.handler_runtime
                    .spawn(se.task_tracker.track_future(async move {
                        filesystem
                            .setlk(
                                &header,
                                nodeid,
                                file_handle,
                                lock_owner,
                                start,
                                end,
                                typ,
                                pid,
                                sleep,
                                reply,
                            )
                            .await;
                    }));
            }
            ll::Operation::BMap(x) => {
                let block_size = x.block_size();
                let block = x.block();
                let reply = self.reply();
                se.handler_runtime
                    .spawn(se.task_tracker.track_future(async move {
                        filesystem
                            .bmap(&header, nodeid, block_size, block, reply)
                            .await;
                    }));
            }
            ll::Operation::IoCtl(x) => {
                if x.unrestricted() {
                    return Err(Errno::ENOSYS);
                }
                let file_handle = x.file_handle();
                let flags = x.flags();
                let command = x.command();
                // TODO: consider using a buffer pool
                let in_data = x.in_data().to_vec();
                let out_size = x.out_size();
                let reply = self.reply();
                se.handler_runtime
                    .spawn(se.task_tracker.track_future(async move {
                        filesystem
                            .ioctl(
                                &header,
                                nodeid,
                                file_handle,
                                flags,
                                command,
                                &in_data,
                                out_size,
                                reply,
                            )
                            .await;
                    }));
            }
            ll::Operation::Poll(x) => {
                let file_handle = x.file_handle();
                let ph = PollNotifier::new(se.ch.sender(), x.kernel_handle());
                let events = x.events();
                let flags = x.flags();
                let reply = self.reply();
                se.handler_runtime
                    .spawn(se.task_tracker.track_future(async move {
                        filesystem
                            .poll(&header, nodeid, file_handle, ph, events, flags, reply)
                            .await;
                    }));
            }
            ll::Operation::NotifyReply(_) => {
                // TODO: handle FUSE_NOTIFY_REPLY
                return Err(Errno::ENOSYS);
            }
            ll::Operation::BatchForget(x) => {
                let nodes: Vec<ForgetOne> = ForgetOne::vec_from_inner(x.nodes());
                se.handler_runtime
                    .spawn(se.task_tracker.track_future(async move {
                        filesystem.batch_forget(&header, &nodes).await;
                    }));
            }
            ll::Operation::FAllocate(x) => {
                let file_handle = x.file_handle();
                let offset = x.offset()?;
                let len = x.len()?;
                let mode = x.mode();
                let reply = self.reply();
                se.handler_runtime
                    .spawn(se.task_tracker.track_future(async move {
                        filesystem
                            .fallocate(&header, nodeid, file_handle, offset, len, mode, reply)
                            .await;
                    }));
            }
            ll::Operation::ReadDirPlus(x) => {
                let file_handle = x.file_handle();
                let offset = x.offset();
                let size = x.size();
                let reply = ReplyDirectoryPlus::new(
                    self.request.unique(),
                    ReplySender::Channel(self.ch.clone()),
                    size as usize,
                );
                se.handler_runtime
                    .spawn(se.task_tracker.track_future(async move {
                        filesystem
                            .readdirplus(&header, nodeid, file_handle, offset, reply)
                            .await;
                    }));
            }
            ll::Operation::Rename2(x) => {
                let from_dir = x.from().dir;
                let from_name = x.from().name.as_os_str().to_os_string();
                let to_dir = x.to().dir;
                let to_name = x.to().name.as_os_str().to_os_string();
                let flags = x.flags();
                let reply = self.reply();
                se.handler_runtime
                    .spawn(se.task_tracker.track_future(async move {
                        filesystem
                            .rename(
                                &header, from_dir, &from_name, to_dir, &to_name, flags, reply,
                            )
                            .await;
                    }));
            }
            ll::Operation::Lseek(x) => {
                let file_handle = x.file_handle();
                let offset = x.offset();
                let whence = x.whence();
                let reply = self.reply();
                se.handler_runtime
                    .spawn(se.task_tracker.track_future(async move {
                        filesystem
                            .lseek(&header, nodeid, file_handle, offset, whence, reply)
                            .await;
                    }));
            }
            ll::Operation::CopyFileRange(x) => {
                let (i, o) = (x.src()?, x.dest()?);
                let len = x.len();
                let flags = x.flags();
                let reply = self.reply();
                se.handler_runtime
                    .spawn(se.task_tracker.track_future(async move {
                        filesystem
                            .copy_file_range(
                                &header,
                                i.inode,
                                i.file_handle,
                                i.offset,
                                o.inode,
                                o.file_handle,
                                o.offset,
                                len,
                                flags,
                                reply,
                            )
                            .await;
                    }));
            }
            #[cfg(target_os = "macos")]
            ll::Operation::SetVolName(x) => {
                let name = x.name().as_os_str().to_os_string();
                let reply = self.reply();
                se.handler_runtime
                    .spawn(se.task_tracker.track_future(async move {
                        filesystem.setvolname(&header, &name, reply).await;
                    }));
            }
            #[cfg(target_os = "macos")]
            ll::Operation::GetXTimes(x) => {
                let reply = self.reply();
                se.handler_runtime
                    .spawn(se.task_tracker.track_future(async move {
                        filesystem.getxtimes(&header, nodeid, reply).await;
                    }));
            }
            #[cfg(target_os = "macos")]
            ll::Operation::Exchange(x) => {
                let from_dir = x.from().dir;
                let from_name = x.from().name.as_os_str().to_os_string();
                let to_dir = x.to().dir;
                let to_name = x.to().name.as_os_str().to_os_string();
                let options = x.options();
                let reply = self.reply();
                se.handler_runtime
                    .spawn(se.task_tracker.track_future(async move {
                        filesystem
                            .exchange(
                                &header, from_dir, &from_name, to_dir, &to_name, options, reply,
                            )
                            .await;
                    }));
            }
            ll::Operation::CuseInit(_) => {
                // TODO: handle CUSE_INIT
                return Err(Errno::ENOSYS);
            }
        }
        Ok(None)
    }

    /// Create a reply object for this request that can be passed to the filesystem
    /// implementation and makes sure that a request is replied exactly once
    pub(crate) fn reply<T: Reply>(&self) -> T {
        Reply::new(self.request.unique(), ReplySender::Channel(self.ch.clone()))
    }

    /// Returns a Request reference for this request
    #[inline]
    fn request_header(&self) -> &Request {
        Request::ref_cast(self.request.header())
    }
}
