//! Spawner implementations, allowing users to customize the filesystem's multithreading mechanisms
use crate::runtime::DroppableRuntime;
use std::{any::Any, io, pin::Pin};
use tokio::runtime::RuntimeFlavor;

/// A spawner is a trait that allows spawning work futures on any compatible async runtime or thread pool.
pub trait Spawner<R>: Any
where
    R: Send + 'static,
    Self: Sync + Send,
{
    /// Check whether the spawner is valid for the session.
    fn is_valid(&self) -> bool;

    /// Spawns a task on the spawner handle and return a joinable task.
    fn spawn(
        &self,
        task_name: &str,
        task: Pin<Box<dyn Future<Output = R> + Send + 'static>>,
    ) -> Result<Box<dyn Joinable<R>>, io::Error>;
}

/// A runtime-agnostic joinable task
#[async_trait::async_trait]
pub trait Joinable<R>
where
    R: Send + 'static,
    Self: Send,
{
    /// Joins the task and returns the result.
    async fn join(self: Box<Self>) -> Result<R, Box<dyn std::error::Error + Sync + Send>>;
}

impl<R> Spawner<R> for tokio::runtime::Handle
where
    R: Send + 'static,
{
    fn is_valid(&self) -> bool {
        self.runtime_flavor() == RuntimeFlavor::MultiThread
    }

    fn spawn(
        &self,
        task_name: &str,
        task: Pin<Box<dyn Future<Output = R> + Send + 'static>>,
    ) -> Result<Box<dyn Joinable<R>>, io::Error> {
        let task = tokio::task::Builder::new()
            .name(task_name)
            .spawn_on(task, self)?;
        Ok(Box::new(task))
    }
}

impl<R> Spawner<R> for DroppableRuntime
where
    R: Send + 'static,
{
    fn is_valid(&self) -> bool {
        self.handle().runtime_flavor() == RuntimeFlavor::MultiThread
    }

    fn spawn(
        &self,
        task_name: &str,
        task: Pin<Box<dyn Future<Output = R> + Send + 'static>>,
    ) -> Result<Box<dyn Joinable<R>>, io::Error> {
        let task = tokio::task::Builder::new()
            .name(task_name)
            .spawn_on(task, self.handle())?;
        Ok(Box::new(task))
    }
}

#[async_trait::async_trait]
impl<R> Joinable<R> for tokio::task::JoinHandle<R>
where
    R: Send + 'static,
{
    async fn join(self: Box<Self>) -> Result<R, Box<dyn std::error::Error + Sync + Send>> {
        Ok(self.await?)
    }
}

impl<R> Spawner<R> for rusty_pool::ThreadPool
where
    R: Send + 'static,
{
    fn is_valid(&self) -> bool {
        true
    }

    fn spawn(
        &self,
        _task_name: &str,
        task: Pin<Box<dyn Future<Output = R> + Send + 'static>>,
    ) -> Result<Box<dyn Joinable<R>>, io::Error> {
        let task = self.spawn_await(task);
        Ok(Box::new(task))
    }
}

#[async_trait::async_trait]
impl<R> Joinable<R> for rusty_pool::JoinHandle<R>
where
    R: Send + 'static,
{
    async fn join(self: Box<Self>) -> Result<R, Box<dyn std::error::Error + Sync + Send>> {
        Ok(self.receiver.await?)
    }
}
