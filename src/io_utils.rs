use std::{
    ops::{Deref, DerefMut},
    os::fd::{AsFd, AsRawFd, FromRawFd, IntoRawFd},
};
use tokio::fs::File;

/// A wrapper that allows temporarily borrowing a file descriptor as a mutable [`File`] object.
/// This allows reading from a file descriptor like pipes without a performance penalty
/// incurred by a mutex.
pub(crate) struct BorrowedFile<'a, T: AsFd> {
    file: Option<File>,
    #[allow(dead_code)] // Reference to the original file needs to be held
    fd: &'a T,
}

impl<'a, T: AsFd> BorrowedFile<'a, T> {
    /// Creates a new [`BorrowedFile`] from a file descriptor.
    ///
    /// # Safety
    ///
    /// This function is unsafe because it creates a [`File`] object from a file descriptor
    /// while allowing the user to break the borrow checker's rules.
    pub(crate) unsafe fn new(fd: &'a T) -> Self {
        Self {
            file: Some(unsafe { File::from_raw_fd(fd.as_fd().as_raw_fd()) }),
            fd,
        }
    }
}

impl<'a, T: AsFd> Deref for BorrowedFile<'a, T> {
    type Target = File;

    fn deref(&self) -> &Self::Target {
        self.file.as_ref().unwrap()
    }
}

impl<'a, T: AsFd> DerefMut for BorrowedFile<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.file.as_mut().unwrap()
    }
}

impl<'a, T: AsFd> Drop for BorrowedFile<'a, T> {
    fn drop(&mut self) {
        let _ = self.file.take().map(|file| {
            file.try_into_std()
                .expect("no file operation should be running now")
                .into_raw_fd()
        });
    }
}
