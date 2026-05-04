use std::{
    io,
    ops::{Deref, DerefMut, Index},
    sync::atomic::{AtomicUsize, Ordering},
};

use tokio::{
    runtime::{Builder, Handle, Runtime, RuntimeFlavor},
    task::JoinHandle,
};

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
    block_on_drop: bool,
}

impl DroppableRuntime {
    pub(crate) fn new(
        worker_name: &str,
        worker_threads: usize,
        block_on_drop: bool,
    ) -> io::Result<Self> {
        Ok(Self {
            runtime: Some(
                tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(worker_threads)
                    .thread_name(worker_name)
                    .enable_all()
                    .build()?,
            ),
            block_on_drop,
        })
    }
}

impl Drop for DroppableRuntime {
    fn drop(&mut self) {
        if self.block_on_drop {
            let rt = self.runtime.take();
            // Trick tokio's runtime by moving out of any async contexts.
            std::thread::spawn(move || {
                drop(rt);
            })
            .join()
            .expect("drop thread failed to join");
        } else {
            self.runtime.take().unwrap().shutdown_background();
        }
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

/// Custom cluster of Tokio runtimes that helps with reducing contention on certain shared runtime states.
#[allow(unused)]
pub(crate) struct DroppableRuntimeCluster {
    shards: Vec<DroppableRuntime>,
    worker_threads: usize,
    worker_threads_per_shard: usize,
    counter: AtomicUsize,
}

#[allow(unused)]
impl DroppableRuntimeCluster {
    pub(crate) fn new(
        worker_name: &str,
        worker_threads: usize,
        worker_threads_per_shard: usize,
        block_on_drop: bool,
    ) -> io::Result<Self> {
        if worker_threads == 0 || worker_threads_per_shard == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "worker_threads and worker_threads_per_shard must be greater than 0",
            ));
        }
        let mut remaining_threads = worker_threads;
        let mut shards = Vec::with_capacity(worker_threads_per_shard);
        while remaining_threads > 0 {
            let thread_count = remaining_threads.min(worker_threads_per_shard);
            shards.push(DroppableRuntime::new(
                worker_name,
                thread_count,
                block_on_drop,
            )?);
            remaining_threads -= thread_count;
        }
        log::info!(
            "Created {} shards with up to {} threads each",
            shards.len(),
            worker_threads_per_shard
        );
        Ok(Self {
            shards,
            worker_threads,
            worker_threads_per_shard,
            counter: AtomicUsize::new(0),
        })
    }

    pub(crate) fn len(&self) -> usize {
        self.shards.len()
    }

    pub(crate) fn spawn<F, R>(&self, future: F) -> JoinHandle<R>
    where
        F: Future<Output = R> + Send + 'static,
        R: Send + 'static,
    {
        let counter_value = self.counter.fetch_add(1, Ordering::Relaxed);
        let thread_index = (counter_value / 10) % self.worker_threads;
        let shard_index = thread_index / self.worker_threads_per_shard;
        self.shards[shard_index].spawn(future)
    }
}

impl Index<usize> for DroppableRuntimeCluster {
    type Output = DroppableRuntime;

    fn index(&self, index: usize) -> &Self::Output {
        &self.shards[index]
    }
}
