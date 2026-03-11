/// How the file should be opened: read-only, write-only, or read-write.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
#[repr(i32)]
#[allow(non_camel_case_types)]
pub enum OpenAccMode {
    /// Open file for reading only.
    O_RDONLY = libc::O_RDONLY,
    /// Open file for writing only.
    O_WRONLY = libc::O_WRONLY,
    /// Open file for reading and writing.
    O_RDWR = libc::O_RDWR,
}

bitflags::bitflags! {
    #[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
    /// Flags used when opening files, as passed into the open operation.
    pub struct OpenFlags: i32 {
        /// Open for reading only.
        const READ_ONLY = libc::O_RDONLY;
        /// Open for writing only.
        const WRITE_ONLY = libc::O_WRONLY;
        /// Open for reading and writing.
        const READ_WRITE = libc::O_RDWR;
        /// Create file if it doesn't exist.
        const CREATE = libc::O_CREAT;
        /// Fail if file already exists.
        const CREATE_EXCLUSIVE = libc::O_EXCL;
        /// Don't assign controlling terminal.
        const NO_TERMINAL_CONTROL = libc::O_NOCTTY;
        /// Truncate file to zero length.
        const TRUNCATE = libc::O_TRUNC;
        /// Set append mode.
        const APPEND_MODE = libc::O_APPEND;
        /// Use non-blocking mode.
        const NON_BLOCKING_MODE = libc::O_NONBLOCK;
        /// Synchronize data writes.
        const SYNC_DATA_ONLY = libc::O_DSYNC;
        /// Synchronize both data and metadata writes.
        const SYNC_DATA_AND_METADATA = libc::O_SYNC;
        /// Synchronize read operations (Linux only).
        #[cfg(target_os = "linux")]
        const SYNC_READS_AND_WRITES = libc::O_RSYNC;
        /// Fail if not a directory.
        const MUST_BE_DIRECTORY = libc::O_DIRECTORY;
        /// Do not follow symlinks.
        const DO_NOT_FOLLOW_SYMLINKS = libc::O_NOFOLLOW;
        /// Set close-on-exec flag.
        const CLOSE_ON_EXEC = libc::O_CLOEXEC;
        /// Create an unnamed temporary file (Linux only).
        #[cfg(target_os = "linux")]
        const TEMPORARY_FILE = libc::O_TMPFILE;
        const _ = !0;
    }
}

impl OpenFlags {
    /// File access mode.
    pub fn acc_mode(self) -> OpenAccMode {
        match self.bits() & libc::O_ACCMODE {
            libc::O_RDONLY => OpenAccMode::O_RDONLY,
            libc::O_WRONLY => OpenAccMode::O_WRONLY,
            libc::O_RDWR => OpenAccMode::O_RDWR,
            _ => {
                // Impossible combination of flags.
                // Do not panic because the field is public.
                OpenAccMode::O_RDONLY
            }
        }
    }
}
