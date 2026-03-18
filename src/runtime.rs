use lazy_static::lazy_static;
use tokio::runtime::{Builder, Handle, Runtime, RuntimeFlavor};

lazy_static! {
    static ref FALLBACK_RUNTIME: Runtime = Builder::new_multi_thread()
        .worker_threads(6)
        .build()
        .expect("should be able to create a temporary runtime");
}

fn get_arbitrary_handle() -> Handle {
    Handle::try_current().unwrap_or_else(|_| FALLBACK_RUNTIME.handle().clone())
}

pub(crate) fn execute_future_from_sync<F: Future>(future: F) -> F::Output
where
    F::Output: Send,
    F: Send,
{
    let handle = get_arbitrary_handle();
    let flavor = handle.runtime_flavor();
    match flavor {
        RuntimeFlavor::CurrentThread => std::thread::scope(|s| {
            // We are forced to spawn a new thread to trick tokio's executor, and execute the future there,
            // at the cost of blocking the current thread and the future and output objects having to be Send.
            s.spawn(move || FALLBACK_RUNTIME.block_on(future))
                .join()
                .unwrap()
        }),
        RuntimeFlavor::MultiThread => tokio::task::block_in_place(|| handle.block_on(future)),
        _ => panic!("unexpected runtime flavor: {:?}", flavor),
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_handle_outside_runtime() {
        assert_eq!(get_arbitrary_handle().id(), FALLBACK_RUNTIME.handle().id());
    }

    #[tokio::test]
    async fn test_handle_inside_runtime() {
        let current_handle = Handle::current();
        assert_eq!(get_arbitrary_handle().id(), current_handle.id());
    }

    #[tokio::test]
    async fn should_execute_future_inside_single_threaded_runtime() {
        execute_future_from_sync(async move { "should execute on single threaded runtime" });
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn should_execute_future_inside_multi_threaded_runtime() {
        execute_future_from_sync(async move { "should execute on multi threaded runtime" });
    }

    #[test]
    fn test_execute_future_outside_runtime() {
        execute_future_from_sync(async move { "should execute outside of runtime context" });
    }
}
