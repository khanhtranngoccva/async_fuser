use async_fuser::{Config, examples::DummyFS};
use clap::{Parser, Subcommand};
use stats_alloc::{INSTRUMENTED_SYSTEM, Region, StatsAlloc};
use std::{alloc::System, num::NonZero};
use tempdir::TempDir;

#[derive(Clone, Debug, Parser)]
struct Args {
    #[command(subcommand)]
    scenario: Scenario,
}

#[derive(Clone, Debug, Subcommand)]
enum Scenario {
    DummyFilesystems {
        #[arg(long, default_value = "1")]
        num_filesystems: NonZero<usize>,
    },
}

#[global_allocator]
pub static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

pub async fn dummy_filesystems(
    num_filesystems: NonZero<usize>,
    ready: tokio::sync::oneshot::Sender<()>,
) {
    let temp_dirs = (0..num_filesystems.get())
        .map(|i| TempDir::new(&format!("dummy-filesystem-{}", i)).unwrap())
        .collect::<Vec<_>>();
    let mut sessions = Vec::new();
    for temp_dir in &temp_dirs {
        let session = async_fuser::spawn_mount(DummyFS, &temp_dir, &Config::default())
            .await
            .unwrap();
        sessions.push(session);
    }
    ready.send(()).expect("failed to send ready signal");
    std::future::pending().await
}

pub async fn trigger_known_globals() {
    let _ = async_hybrid_fs::default_client();
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let args = Args::parse();
    trigger_known_globals().await;
    let mem_initial = memory_stats::memory_stats().expect("failed to get memory stats");
    let mut r = Region::new(GLOBAL);
    r.reset();
    let (ready, ready_rx) = tokio::sync::oneshot::channel();
    let task = match args.scenario {
        Scenario::DummyFilesystems { num_filesystems } => {
            tokio::spawn(dummy_filesystems(num_filesystems, ready))
        }
    };
    ready_rx.await.expect("failed to receive ready signal");
    let mem_final = memory_stats::memory_stats().expect("failed to get memory stats");
    let stats = r.change();
    let stats_alloc_mb_allocated = stats.bytes_allocated as f64 / 1024f64 / 1024f64;
    let stats_alloc_mb_deallocated = stats.bytes_deallocated as f64 / 1024f64 / 1024f64;
    let stats_alloc_mb_delta =
        (stats.bytes_allocated as i64 - stats.bytes_deallocated as i64) as f64 / 1024f64 / 1024f64;
    let memory_stats_mb_initial = mem_initial.virtual_mem as f64 / 1024f64 / 1024f64;
    let memory_stats_mb_final = mem_final.virtual_mem as f64 / 1024f64 / 1024f64;
    let memory_stats_mb_delta =
        (mem_final.virtual_mem as i64 - mem_initial.virtual_mem as i64) as f64 / 1024f64 / 1024f64;

    println!("stats_alloc: allocated {} MB", stats_alloc_mb_allocated);
    println!("stats_alloc: deallocated {} MB", stats_alloc_mb_deallocated);
    println!("stats_alloc: delta {} MB", stats_alloc_mb_delta);
    println!("memory_stats: initial {} MB", memory_stats_mb_initial);
    println!("memory_stats: final {} MB", memory_stats_mb_final);
    println!("memory_stats: delta {} MB", memory_stats_mb_delta);

    task.abort();
    let _ = task.await;
    Ok(())
}
