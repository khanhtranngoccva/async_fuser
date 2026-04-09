use std::ops::{Deref, DerefMut};

use tokio::runtime::{Builder, Handle, Runtime, RuntimeFlavor};

pub(crate) fn execute_future_from_sync<F>(future: F) -> F::Output
where
    F::Output: Send,
    F: Future + Send,
{
    let handle = Handle::try_current().ok();
    match handle {
        Some(handle) => match handle.runtime_flavor() {
            RuntimeFlavor::CurrentThread => std::thread::scope(|s| {
                s.spawn(move || {
                    let backup_runtime = Builder::new_multi_thread().enable_all().build().unwrap();
                    backup_runtime.block_on(future)
                })
                .join()
                .unwrap()
            }),
            RuntimeFlavor::MultiThread => {
                tokio::task::block_in_place(move || handle.block_on(future))
            }
            _ => {
                unreachable!("Unsupported runtime flavor: {:?}", handle.runtime_flavor())
            }
        },
        None => {
            let backup_runtime = Builder::new_multi_thread().enable_all().build().unwrap();
            tokio::task::block_in_place(move || backup_runtime.block_on(future))
        }
    }
}

#[derive(Debug)]
pub(crate) struct DroppableRuntime {
    runtime: Option<Runtime>,
}

impl DroppableRuntime {
    pub(crate) fn new(worker_threads: usize) -> Self {
        Self {
            runtime: Some(
                tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(worker_threads)
                    .enable_all()
                    .build()
                    .unwrap(),
            ),
        }
    }
}

impl Drop for DroppableRuntime {
    fn drop(&mut self) {
        self.runtime.take().unwrap().shutdown_background();
    }
}

impl Deref for DroppableRuntime {
    type Target = Runtime;

    fn deref(&self) -> &Self::Target {
        self.runtime.as_ref().unwrap()
    }
}

impl DerefMut for DroppableRuntime {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.runtime.as_mut().unwrap()
    }
}
