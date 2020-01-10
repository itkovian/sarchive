/*
Copyright 2019-2020 Andy Georges <itkovian+sarchive@gmail.com>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

pub mod job;
pub mod slurm;
pub mod torque;

use clap::ArgMatches;
use notify::event::Event;
use std::path::{Path, PathBuf};

use job::JobInfo;

/// Denotes the schedulers SArchive supports
pub enum SchedulerKind {
    Slurm,
    Torque,
}

pub trait Scheduler: Send + Sync {
    fn watch_locations(&self, matches: &ArgMatches) -> Vec<PathBuf>;
    fn create_job_info(&self, event_path: &Path) -> Option<Box<dyn JobInfo>>;
    fn verify_event_kind(&self, event: &Event) -> Option<Vec<PathBuf>>;
}

pub fn create(kind: &SchedulerKind, spool_path: &PathBuf, cluster: &str) -> Box<dyn Scheduler> {
    match kind {
        SchedulerKind::Slurm => Box::new(slurm::Slurm::new(spool_path, cluster)),
        SchedulerKind::Torque => Box::new(torque::Torque::new(spool_path, cluster)),
    }
}

#[cfg(test)]
mod tests {}
