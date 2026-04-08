use tokio::runtime::{Builder, Handle, RuntimeFlavor};

/// Attempts to execute a future on a temporary runtime (based on async_scoped's TokioScope implementation).
pub(crate) fn execute_future_from_sync<F>(future: F) -> F::Output
where
    F::Output: Send,
    F: Future + Send,
{
    let handle_flavor = Handle::try_current().ok().map(|r| r.runtime_flavor());
    let backup_runtime = Builder::new_current_thread().build().unwrap();
    if let Some(RuntimeFlavor::CurrentThread) = handle_flavor {
        std::thread::scope(|s| {
            s.spawn(move || backup_runtime.block_on(future))
                .join()
                .unwrap()
        })
    } else {
        tokio::task::block_in_place(|| backup_runtime.block_on(future))
    }
}
