//! Filesystem examples (used for unit testing and integration benchmarking)
use crate::Filesystem;

/// Dummy filesystem implementation that does nothing. Used for testing basic session functionalities and determining baseline memory footprint.
#[derive(Debug)]
pub struct DummyFS;

impl Filesystem for DummyFS {}
