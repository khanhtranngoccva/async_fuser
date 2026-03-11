mod fuse_pure;
pub(crate) mod mount_options;
pub(crate) mod unmount_options;

use crate::{dev_fuse::DevFuse, session::SessionACL};
use mount_options::MountOption;
use std::{
    io,
    path::{Path, PathBuf},
    sync::Arc,
};
use unmount_options::UnmountOption;

#[derive(Debug)]
enum MountImpl {
    Pure(fuse_pure::MountImpl),
}

impl MountImpl {
    async fn is_alive(&self) -> bool {
        match self {
            MountImpl::Pure(mount) => mount.is_alive().await,
        }
    }

    async fn umount_impl(&mut self, flags: &[UnmountOption]) -> io::Result<()> {
        match self {
            MountImpl::Pure(mount) => mount.umount_impl(flags).await,
        }
    }
}

#[derive(Debug)]
pub(crate) struct Mount {
    mount_impl: Option<MountImpl>,
    #[allow(dead_code)] // Debugging purposes
    mount_point: PathBuf,
}

impl Mount {
    pub(crate) async fn new(
        mountpoint: &Path,
        options: &[MountOption],
        acl: SessionACL,
    ) -> io::Result<(Arc<DevFuse>, Mount)> {
        let (dev_fuse, mount) = fuse_pure::MountImpl::new(mountpoint, options, acl).await?;
        Ok((
            dev_fuse,
            Mount {
                mount_impl: Some(MountImpl::Pure(mount)),
                mount_point: mountpoint.to_owned(),
            },
        ))
    }

    pub(crate) async fn umount(
        mut self,
        flags: &[UnmountOption],
    ) -> Result<(), (Option<Self>, io::Error)> {
        let mount_impl = match self.mount_impl.as_mut() {
            Some(mount) => mount,
            None => return Ok(()),
        };
        match unmount_options::check_option_conflicts(flags) {
            Ok(()) => (),
            Err(err) => return Err((Some(self), err)),
        };
        if let Err(err) = mount_impl.umount_impl(flags).await {
            let salvaged = is_mount_salvageable(&err) && mount_impl.is_alive().await;
            if !salvaged {
                self.mount_impl = None;
            }
            return Err((salvaged.then_some(self), err));
        }
        // This prevents the mount from being removed twice.
        self.mount_impl = None;
        Ok(())
    }
}

// FIXME: Integrate async if possible
async fn is_mounted(fuse_device: &DevFuse) -> bool {
    use std::os::unix::io::AsFd;
    use std::slice;

    use nix::poll::PollFd;
    use nix::poll::PollFlags;
    use nix::poll::PollTimeout;
    use nix::poll::poll;

    loop {
        let mut poll_fd = PollFd::new(fuse_device.as_fd(), PollFlags::empty());
        let res = poll(slice::from_mut(&mut poll_fd), PollTimeout::ZERO);
        break match res {
            Ok(0) => true,
            Ok(1) => poll_fd
                .revents()
                .is_some_and(|r| r.contains(PollFlags::POLLERR)),
            Ok(_) => unreachable!(),
            Err(nix::errno::Errno::EINTR) => continue,
            Err(err) => {
                // This should never happen. The fd is guaranteed good as `File` owns it.
                // According to man poll ENOMEM is the only error code unhandled, so we panic
                // consistent with rust's usual ENOMEM behaviour.
                panic!("Poll failed with error {err}")
            }
        };
    }
}

// FIXME: Integrate async if possible
async fn libc_umount(mnt: &Path, flags: &[UnmountOption]) -> nix::Result<()> {
    let nix_flags =
        nix::mount::MntFlags::from_bits_retain(unmount_options::to_unmount_syscall(flags));
    #[cfg(any(
        target_os = "macos",
        target_os = "freebsd",
        target_os = "dragonfly",
        target_os = "openbsd",
        target_os = "netbsd"
    ))]
    {
        nix::mount::unmount(mnt, nix_flags)?;
        Ok(())
    }
    #[cfg(not(any(
        target_os = "macos",
        target_os = "freebsd",
        target_os = "dragonfly",
        target_os = "openbsd",
        target_os = "netbsd"
    )))]
    {
        nix::mount::umount2(mnt, nix_flags)?;
        Ok(())
    }
}

/// Default unmount flags for the current platform, used as a fallback when umount() is not explicitly called.
pub(crate) fn drop_umount_flags() -> &'static [UnmountOption] {
    {
        #[cfg(target_os = "linux")]
        {
            &[UnmountOption::Detach]
        }
        #[cfg(target_os = "macos")]
        {
            &[UnmountOption::Force]
        }
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        {
            &[]
        }
    }
}

/// Determines whether a mount can be salvaged after an error.
fn is_mount_salvageable(err: &io::Error) -> bool {
    match err.kind() {
        io::ErrorKind::ResourceBusy => return true,
        io::ErrorKind::WouldBlock => return true,
        io::ErrorKind::InvalidInput => return false,
        io::ErrorKind::PermissionDenied => return true,
        io::ErrorKind::NotFound => return false,
        io::ErrorKind::OutOfMemory => return true,
        _ => {}
    };
    match err.raw_os_error() {
        Some(libc::EBUSY) => true,
        Some(libc::EAGAIN) => true,
        Some(libc::EFAULT) => false,
        Some(libc::EINVAL) => false,
        Some(libc::ENAMETOOLONG) => false,
        Some(libc::ENOENT) => true,
        Some(libc::ENOMEM) => true,
        Some(libc::EPERM) => true,
        _ => false,
    }
}
