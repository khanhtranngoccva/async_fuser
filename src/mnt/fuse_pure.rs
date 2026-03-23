//! Native FFI bindings to libfuse.
//!
//! This is a small set of bindings that are required to mount/unmount FUSE filesystems and
//! open/close a fd to the FUSE kernel driver.

use regex::bytes::Regex;
use std::env;
use std::ffi::OsStr;
use std::ffi::OsString;
use std::io;
use std::io::Error;
use std::io::ErrorKind;
use std::io::IoSliceMut;
use std::mem;
use std::os::fd::AsFd;
use std::os::fd::BorrowedFd;
use std::os::fd::OwnedFd;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::io::AsRawFd;
use std::os::unix::io::FromRawFd;
use std::os::unix::io::RawFd;
use std::path::Path;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tokio::net::UnixStream;
use tokio::process::Command;

use log::debug;
use log::error;
use nix::fcntl::FcntlArg;
use nix::fcntl::FdFlag;
use nix::fcntl::OFlag;
use nix::fcntl::fcntl;
use nix::sys::socket::ControlMessageOwned;
use nix::sys::socket::MsgFlags;
use nix::sys::socket::SockaddrStorage;
use nix::sys::socket::recvmsg;

use crate::SessionACL;
use crate::dev_fuse::DevFuse;
use crate::ll::errno;
use crate::mnt::is_mounted;
use crate::mnt::mount_options::MountOption;
use crate::mnt::mount_options::MountOptionGroup;
use crate::mnt::mount_options::option_group;
use crate::mnt::mount_options::option_to_flag;
use crate::mnt::mount_options::option_to_string;
use crate::mnt::unmount_options;
use crate::mnt::unmount_options::UnmountOption;
use crate::runtime;

const FUSERMOUNT_BIN: &str = "fusermount";
const FUSERMOUNT3_BIN: &str = "fusermount3";
const FUSERMOUNT_COMM_ENV: &str = "_FUSE_COMMFD";
const MOUNT_FUSEFS_BIN: &str = "mount_fusefs";

#[derive(Debug)]
pub(crate) struct MountImpl {
    state: Option<MountState>,
}

#[derive(Debug)]
struct MountState {
    mountpoint: PathBuf,
    #[allow(dead_code)]
    auto_unmount_socket: Option<UnixStream>,
    fuse_device: Arc<DevFuse>,
}

async fn unmount_state_obj(
    state: &mut Option<MountState>,
    flags: &[UnmountOption],
) -> io::Result<()> {
    let state_internal = match state.as_mut() {
        Some(state) => state,
        None => return Ok(()),
    };
    if !is_mounted(&state_internal.fuse_device).await {
        // If the filesystem has already been unmounted, avoid unmounting it again.
        // Unmounting it a second time could cause a race with a newly mounted filesystem
        // living at the same mountpoint
        *state = None;
        return Ok(());
    }
    if let Err(err) = super::libc_umount(&state_internal.mountpoint, flags).await {
        // If the filesystem is gone, we need to clear the state and prevent the
        // unmount function from being called again.
        if !is_mounted(&state_internal.fuse_device).await {
            *state = None;
            return Err(err.into());
        }
        // Linux always returns EPERM for non-root users.  We have to let the
        // library go through the setuid-root "fusermount -u" to unmount.
        else if err == nix::errno::Errno::EPERM {
            if let Err(e) = fuse_unmount_pure(&state_internal.mountpoint, flags).await {
                if !is_mounted(&state_internal.fuse_device).await {
                    *state = None;
                }
                return Err(e);
            };
            *state = None;
            return Ok(());
        } else {
            return Err(err.into());
        }
    }
    // If the unmount was successful, we must clear the state.
    *state = None;
    Ok(())
}

impl MountImpl {
    pub(crate) async fn new(
        mountpoint: &Path,
        options: &[MountOption],
        acl: SessionACL,
    ) -> io::Result<(Arc<DevFuse>, MountImpl)> {
        let mountpoint = mountpoint.canonicalize()?;
        let (file, sock) = fuse_mount_pure(mountpoint.as_os_str(), options, acl).await?;
        let file = Arc::new(file);
        Ok((
            file.clone(),
            MountImpl {
                state: Some(MountState {
                    mountpoint: mountpoint.to_path_buf(),
                    auto_unmount_socket: sock,
                    fuse_device: file,
                }),
            },
        ))
    }

    pub(crate) async fn is_alive(&self) -> bool {
        let state = self.state.as_ref();
        if state.is_none() {
            return false;
        }
        is_mounted(&state.unwrap().fuse_device).await
    }

    pub(crate) async fn umount_impl(&mut self, flags: &[UnmountOption]) -> io::Result<()> {
        unmount_state_obj(&mut self.state, flags).await
    }
}

impl Drop for MountImpl {
    fn drop(&mut self) {
        let mut owned_state = self.state.take();
        if owned_state.is_none() {
            return;
        }
        let flags = super::drop_umount_flags();
        let mountpoint = owned_state.as_ref().unwrap().mountpoint.clone();
        // Use a temporary runtime to force a proper cleanup. Most of the time, the caller should call the
        // async umount_impl method to unmount the filesystem. In the future, the AsyncDrop trait should be used
        runtime::execute_future_from_sync(async move {
            while owned_state.is_some() {
                match unmount_state_obj(&mut owned_state, &flags).await {
                    Ok(()) => return,
                    Err(err) => {
                        let err_kind = err.kind();
                        if err_kind == io::ErrorKind::ResourceBusy {
                            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                        } else if err_kind == io::ErrorKind::WouldBlock {
                            tokio::time::sleep(std::time::Duration::from_secs_f64(0.01)).await;
                        } else {
                            log::error!("Error unmounting filesystem at {:?}: {err}", mountpoint);
                            return;
                        }
                    }
                }
            }
        });
    }
}

async fn fuse_mount_pure(
    mountpoint: &OsStr,
    options: &[MountOption],
    acl: SessionACL,
) -> Result<(DevFuse, Option<UnixStream>), io::Error> {
    if options.contains(&MountOption::AutoUnmount) {
        // Auto unmount is only supported via fusermount
        return fuse_mount_fusermount(mountpoint, options, acl).await;
    }

    // The direct mount path is currently implemented only for Linux and macOS.
    // Other supported Unix targets (such as the BSDs) rely on the setuid
    // mount helper, which mirrors libfuse's approach.
    if cfg!(target_os = "linux") || cfg!(target_os = "macos") {
        let res = fuse_mount_sys(mountpoint, options, acl).await?;
        match res {
            Some(file) => return Ok((file, None)),
            None => {
                // Retry
            }
        }
    }

    fuse_mount_fusermount(mountpoint, options, acl).await
}

async fn fuse_unmount_pure(mountpoint: &Path, flags: &[UnmountOption]) -> io::Result<()> {
    let fusermount_bin = detect_fusermount_bin().await;
    let mut builder = Command::new(&fusermount_bin);
    builder.stdout(Stdio::piped()).stderr(Stdio::piped());
    builder.arg("-u");
    for flag in flags {
        if let Some(cmd_arg) = unmount_options::to_fusermount_option(flag) {
            builder.arg(cmd_arg);
        }
    }
    builder
        .arg("--")
        .arg(OsStr::new(&mountpoint.to_string_lossy().into_owned()));
    let output = builder.output().await?;
    debug!(
        "fusermount stdout on {}: {}",
        mountpoint.display(),
        String::from_utf8_lossy(&output.stdout)
    );
    debug!(
        "fusermount stderr on {}: {}",
        mountpoint.display(),
        String::from_utf8_lossy(&output.stderr)
    );
    if output.status.success() {
        return Ok(());
    }
    let fusermount_error = parse_fusermount_unmount_stderr(OsStr::from_bytes(&output.stderr))
        .ok_or_else(|| {
            io::Error::new(
                ErrorKind::Other,
                format!(
                    "Failed to parse fusermount umount error message: {}",
                    String::from_utf8_lossy(&output.stderr)
                ),
            )
        })?;
    // Since `fusermount` does not invoke any locale functions,
    // the locale used for `strerror` in the program is guaranteed to be `C`.
    let errno = errno::get_errno_by_message(
        &fusermount_error,
        &"C".try_into().expect("locale should be valid"),
    )
    .map_err(|e| {
        error!("failed to get errno by fusermount umount message: {}", e);
        io::Error::new(
            ErrorKind::Other,
            "failed to get errno by fusermount umount message",
        )
    })?
    .ok_or_else(|| {
        error!(
            "errno not found for fusermount umount message: {:?}",
            fusermount_error
        );
        io::Error::new(
            ErrorKind::Other,
            "errno not found for fusermount umount message",
        )
    })?;
    Err(Error::from_raw_os_error(errno.code()))
}

async fn detect_fusermount_bin() -> String {
    if let Some(fusermount) = env::var_os("FUSERMOUNT_PATH") {
        return fusermount
            .to_str()
            .expect("FUSERMOUNT_PATH is not UTF-8")
            .to_owned();
    }

    for name in [
        FUSERMOUNT3_BIN.to_string(),
        FUSERMOUNT_BIN.to_string(),
        MOUNT_FUSEFS_BIN.to_string(),
        format!("/sbin/{FUSERMOUNT3_BIN}"),
        format!("/sbin/{FUSERMOUNT_BIN}"),
        format!("/sbin/{MOUNT_FUSEFS_BIN}"),
        format!("/bin/{FUSERMOUNT3_BIN}"),
        format!("/bin/{FUSERMOUNT_BIN}"),
    ]
    .iter()
    {
        if Command::new(name).arg("-h").output().await.is_ok() {
            return name.to_string();
        }
    }
    // Default to fusermount3
    FUSERMOUNT3_BIN.to_string()
}

fn parse_fusermount_unmount_stderr(output: &OsStr) -> Option<OsString> {
    let parse_regex = Regex::new(r"([^:]+): failed to unmount ([^:]+): (.+)")
        .expect("built-in regex should be valid");
    parse_regex.captures(output.as_bytes()).map(|captures| {
        let error = captures.get(3).map(|m| m.as_bytes()).unwrap_or_default();
        OsStr::from_bytes(error).to_os_string()
    })
}

// FIXME: Integrate async if possible
async fn receive_fusermount_message(socket: &UnixStream) -> Result<DevFuse, Error> {
    let mut io_vec_buf = [0u8];
    let mut iov = [IoSliceMut::new(&mut io_vec_buf)];
    let mut cmsg_buffer = nix::cmsg_space!(RawFd);

    let msg = loop {
        match recvmsg::<SockaddrStorage>(
            socket.as_raw_fd(),
            &mut iov,
            Some(&mut cmsg_buffer),
            MsgFlags::empty(),
        ) {
            Ok(msg) => break msg,
            Err(nix::errno::Errno::EINTR) => continue,
            Err(e) => return Err(e.into()),
        }
    };

    if msg.bytes == 0 {
        return Err(Error::new(
            ErrorKind::UnexpectedEof,
            "Unexpected EOF reading from fusermount",
        ));
    }

    for cmsg in msg
        .cmsgs()
        .map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?
    {
        match cmsg {
            ControlMessageOwned::ScmRights(fds) => {
                if let Some(&fd) = fds.first() {
                    if fd < 0 {
                        return Err(ErrorKind::InvalidData.into());
                    }
                    return Ok(DevFuse::try_from_fd(unsafe { OwnedFd::from_raw_fd(fd) }).await?);
                }
            }
            other => {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!("Unknown control message from fusermount: {:?}", other),
                ));
            }
        }
    }

    Err(Error::new(
        ErrorKind::InvalidData,
        "No SCM_RIGHTS message received from fusermount",
    ))
}

/// Clear `FD_CLOEXEC` after fork before exec.
/// This is needed to pass the file descriptor to a child process without risking descriptor leak.
unsafe fn clear_cloexec_in_pre_exec(command: &mut Command, fd: BorrowedFd<'_>) {
    let fd = fd.as_raw_fd();
    unsafe {
        command.pre_exec(move || {
            let fd = BorrowedFd::borrow_raw(fd);
            let current_flags = fcntl(fd, FcntlArg::F_GETFD)?;
            let current_flags = FdFlag::from_bits_retain(current_flags);
            if current_flags.contains(FdFlag::FD_CLOEXEC) {
                let cleared = current_flags & !FdFlag::FD_CLOEXEC;
                fcntl(fd, FcntlArg::F_SETFD(cleared))?;
            }
            Ok(())
        })
    };
}

// FIXME: Integrate async if possible for fcntl
async fn fuse_mount_fusermount(
    mountpoint: &OsStr,
    options: &[MountOption],
    acl: SessionACL,
) -> Result<(DevFuse, Option<UnixStream>), Error> {
    let fusermount_bin = detect_fusermount_bin().await;

    if fusermount_bin.ends_with(MOUNT_FUSEFS_BIN) {
        return fuse_mount_mount_fusefs(&fusermount_bin, mountpoint, options).await;
    }

    let (child_socket, receive_socket) = UnixStream::pair()?;

    let mut builder = Command::new(&fusermount_bin);
    builder.stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut options_strs: Vec<String> = options.iter().map(option_to_string).collect();
    options_strs.extend(acl.to_mount_option().map(|s| s.to_owned()));
    if !options_strs.is_empty() {
        builder.arg("-o");
        builder.arg(options_strs.join(","));
    }
    builder
        .arg("--")
        .arg(mountpoint)
        .env(FUSERMOUNT_COMM_ENV, child_socket.as_raw_fd().to_string());

    unsafe {
        clear_cloexec_in_pre_exec(&mut builder, child_socket.as_fd());
    }

    let fusermount_child = builder.spawn()?;

    drop(child_socket); // close socket in parent

    let file = match receive_fusermount_message(&receive_socket).await {
        Ok(f) => f,
        Err(_) => {
            // Drop receive socket, since fusermount has exited with an error
            drop(receive_socket);
            let output = fusermount_child.wait_with_output().await?;
            let stderr_string = String::from_utf8_lossy(&output.stderr).to_string();
            return if stderr_string.contains("only allowed if 'user_allow_other' is set") {
                Err(io::Error::new(ErrorKind::PermissionDenied, stderr_string))
            } else {
                Err(io::Error::new(ErrorKind::Other, stderr_string))
            };
        }
    };
    let mut receive_socket = Some(receive_socket);

    if !options.contains(&MountOption::AutoUnmount) {
        // Only close the socket, if auto unmount is not set.
        // fusermount will keep running until the socket is closed, if auto unmount is set
        drop(mem::take(&mut receive_socket));
        let output = fusermount_child.wait_with_output().await?;
        debug!("fusermount: {}", String::from_utf8_lossy(&output.stdout));
        debug!("fusermount: {}", String::from_utf8_lossy(&output.stderr));
    } else {
        if let Some(mut stdout) = fusermount_child.stdout {
            // TODO: do not ignore error.
            if let Ok(flags) = fcntl(&stdout, FcntlArg::F_GETFL) {
                let new_flags = OFlag::from_bits_retain(flags) | OFlag::O_NONBLOCK;
                let _ = fcntl(&stdout, FcntlArg::F_SETFL(new_flags));
            }
            let mut buf = vec![0; 64 * 1024];
            if let Ok(len) = stdout.read(&mut buf).await {
                debug!("fusermount: {}", String::from_utf8_lossy(&buf[..len]));
            }
        }
        if let Some(mut stderr) = fusermount_child.stderr {
            // TODO: do not ignore error.
            if let Ok(flags) = fcntl(&stderr, FcntlArg::F_GETFL) {
                let new_flags = OFlag::from_bits_retain(flags) | OFlag::O_NONBLOCK;
                let _ = fcntl(&stderr, FcntlArg::F_SETFL(new_flags));
            }
            let mut buf = vec![0; 64 * 1024];
            if let Ok(len) = stderr.read(&mut buf).await {
                debug!("fusermount: {}", String::from_utf8_lossy(&buf[..len]));
            }
        }
    }

    // TODO: do not ignore error.
    let _ = fcntl(&file, FcntlArg::F_SETFD(FdFlag::FD_CLOEXEC));

    Ok((file, receive_socket))
}

async fn fuse_mount_mount_fusefs(
    fusermount_bin: &str,
    mountpoint: &OsStr,
    options: &[MountOption],
) -> Result<(DevFuse, Option<UnixStream>), Error> {
    let fuse_device = DevFuse::open().await?;

    let fuse_fd = fuse_device.as_raw_fd();

    let mut builder = Command::new(fusermount_bin);
    builder.stdout(Stdio::piped()).stderr(Stdio::piped());
    if !options.is_empty() {
        builder.arg("-o");
        let options_strs: Vec<String> = options.iter().map(option_to_string).collect();
        builder.arg(options_strs.join(","));
    }

    builder.arg(fuse_fd.to_string()).arg(mountpoint);

    unsafe { clear_cloexec_in_pre_exec(&mut builder, fuse_device.as_fd()) };

    let output = builder.output().await?;
    if !output.status.success() {
        return Err(io::Error::new(
            ErrorKind::Other,
            String::from_utf8_lossy(&output.stderr).to_string(),
        ));
    }

    Ok((fuse_device, None))
}

// If returned option is none. Then fusermount binary should be tried
#[cfg(any(target_os = "linux", target_os = "macos"))]
async fn fuse_mount_sys(
    mountpoint: &OsStr,
    options: &[MountOption],
    acl: SessionACL,
) -> Result<Option<DevFuse>, Error> {
    use std::os::unix::fs::PermissionsExt;

    let mountpoint_mode = File::open(mountpoint)
        .await?
        .metadata()
        .await?
        .permissions()
        .mode();

    // Auto unmount requests must be sent to fusermount binary
    assert!(!options.contains(&MountOption::AutoUnmount));

    let file = match DevFuse::open().await {
        Ok(dev_fuse) => dev_fuse,
        Err(error) => {
            if error.kind() == ErrorKind::NotFound {
                error!("{} not found. Try 'modprobe fuse'", DevFuse::PATH);
            }
            return Err(error);
        }
    };

    assert!(
        file.as_raw_fd() > 2,
        "Conflict with stdin/stdout/stderr. fd={}",
        file.as_raw_fd()
    );

    let mut mount_options = format!(
        "fd={},rootmode={:o},user_id={},group_id={}",
        file.as_raw_fd(),
        mountpoint_mode,
        nix::unistd::getuid(),
        nix::unistd::getgid()
    );

    for option in options
        .iter()
        .filter(|x| option_group(x) == MountOptionGroup::KernelOption)
    {
        mount_options.push(',');
        mount_options.push_str(&option_to_string(option));
    }
    if let Some(acl_option) = acl.to_mount_option() {
        mount_options.push(',');
        mount_options.push_str(acl_option);
    }

    #[cfg(target_os = "linux")]
    let mut flags = nix::mount::MsFlags::empty();
    #[cfg(target_os = "macos")]
    let mut flags = nix::mount::MntFlags::empty();

    if !options.contains(&MountOption::Dev) {
        // Default to nodev
        #[cfg(target_os = "linux")]
        {
            flags |= nix::mount::MsFlags::MS_NODEV;
        }
        #[cfg(target_os = "macos")]
        {
            flags |= nix::mount::MntFlags::MNT_NODEV;
        }
    }
    if !options.contains(&MountOption::Suid) {
        // Default to nosuid
        #[cfg(target_os = "linux")]
        {
            flags |= nix::mount::MsFlags::MS_NOSUID;
        }
        #[cfg(target_os = "macos")]
        {
            flags |= nix::mount::MntFlags::MNT_NOSUID;
        }
    }
    for flag in options
        .iter()
        .filter(|x| option_group(x) == MountOptionGroup::KernelFlag)
    {
        flags |= option_to_flag(flag)?;
    }

    // Default name is "/dev/fuse", then use the subtype, and lastly prefer the name
    let mut source = DevFuse::PATH;
    if let Some(MountOption::Subtype(subtype)) = options
        .iter()
        .find(|x| matches!(**x, MountOption::Subtype(_)))
    {
        source = subtype;
    }
    if let Some(MountOption::FSName(name)) = options
        .iter()
        .find(|x| matches!(**x, MountOption::FSName(_)))
    {
        source = name;
    }

    #[cfg(target_os = "linux")]
    let result = nix::mount::mount(
        Some(source),
        mountpoint,
        Some("fuse"),
        flags,
        Some(mount_options.as_str()),
    );
    #[cfg(target_os = "macos")]
    let result = nix::mount::mount(source, mountpoint, flags, Some(mount_options.as_str()));

    match result {
        Ok(()) => Ok(Some(file)),
        Err(nix::errno::Errno::EPERM) => Ok(None), // Retry with fusermount
        Err(e) => Err(Error::new(
            ErrorKind::Other,
            format!(
                "Error calling mount() at {mountpoint:?} with {mount_options:?} and flags={flags:?}: {e}"
            ),
        )),
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn fuse_mount_sys(
    _mountpoint: &OsStr,
    _options: &[MountOption],
    _acl: SessionACL,
) -> Result<Option<DevFuse>, Error> {
    Ok(None)
}
