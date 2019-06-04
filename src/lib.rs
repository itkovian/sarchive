/*
Copyright 2019 Andy Georges <itkovian+sarchive@gmail.com>

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
extern crate chrono;
extern crate crossbeam_channel;
extern crate crossbeam_utils;

use crossbeam_channel::{Receiver, Sender};
use log::*;
use notify::{DebouncedEvent, RecommendedWatcher, RecursiveMode, Watcher};
use std::fs::create_dir_all;
use std::io::Error;
use std::path::{Path, PathBuf};
use std::sync::mpsc::channel;
use std::time::Duration;
use std::marker::Send;


/// Trait to represent a scheduler item that needs to be archivable
pub trait Scheduler: Send {
    /// Checks that the path is valid for the job item that needs to 
    /// be archived.
    fn valid_path(&self, path: &Path) -> Option<Box<SchedulerJob>>;
}


/// Trait to represent the job information that needs archival
pub trait SchedulerJob: Send {
    /// Archive the files representing this job
    fn archive(&self, archive_path: &Path, period: &Period) -> Result<(), std::io::Error>;
}

/// An enum to define a hierachy in the archive
pub enum Period {
    /// Leads to a YYYYMMDD subdir
    Daily,
    /// Leads to a YYYYMM subdir
    Monthly,
    /// Leads to a YYYY subdir
    Yearly,
    /// No subdir
    None,
}

/// Determines the target path for the job files
///
/// The path will have the following components:
/// - the archive path
/// - a subdir depending on the Period
///     - YYYY in case of a Yearly Period
///     - YYYYMM in case of a Monthly Period
///     - YYYYMMDD in case of a Daily Period 
/// - a file with the given filename
pub fn determine_target_path(archive_path: &Path, p: &Period, job_id: &str, filename: &str) -> PathBuf {
    let archive_subdir = match p {
        Period::Yearly => Some(format!("{}", chrono::Local::now().format("%Y"))),
        Period::Monthly => Some(format!("{}", chrono::Local::now().format("%Y%m"))),
        Period::Daily => Some(format!("{}", chrono::Local::now().format("%Y%m%d"))),
        _ => None
    };
    debug!("Archive subdir is {:?}", &archive_subdir);
    match archive_subdir {
        Some(d) => {
            let archive_subdir_path = archive_path.join(&d);
            if !Path::exists(&archive_subdir_path) {
                debug!("Archive subdir {:?} does not yet exist, creating", &d);
                create_dir_all(&archive_subdir_path).unwrap();
            }
            archive_subdir_path.clone().join(format!("job.{}_{}", job_id, &filename))
        },
        None => archive_path.join(format!("job.{}_{}", job_id, &filename))
    }
}

fn check_and_queue(scheduler: &Scheduler, s: &Sender<Box<SchedulerJob>>, event: DebouncedEvent) -> Result<(), Error> {
    debug!("Event received {:?}", event);
    match event {
        DebouncedEvent::Create(path) | DebouncedEvent::Write(path) => {
            if let Some(job_entry) = scheduler.valid_path(&path) {
                s.send(job_entry).unwrap();
            };
        }
        // We ignore all other events
        _ => (),
    }
    Ok(())
}

pub fn monitor(scheduler: &Scheduler, path: &Path, s: &Sender<Box<SchedulerJob>>) -> notify::Result<()> {
    let (tx, rx) = channel();

    // create a platform-specific watcher
    let mut watcher: RecommendedWatcher = Watcher::new(tx, Duration::from_secs(2))?;
    info!("Watching path {:?}", &path);

    if let Err(e) = watcher.watch(&path, RecursiveMode::NonRecursive) { return Err(e); } 
    loop {
        match rx.recv() {
            Ok(event) => check_and_queue(scheduler, s, event)?,
            Err(e) => {
                error!("Error on received event: {:?}", e);
                break;
            }
        };
    }
    Ok(())
}

pub fn process(archive_path: &Path, p: Period, r: &Receiver<Box<SchedulerJob>>) {
    loop {
        match r.recv() {
            Ok(job_entry) => job_entry.archive(&archive_path, &p),
            Err(_) => {
                error!("Error on receiving SlurmJobEntry info");
                break;
            }
        };
    };
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_determine_target_path() {

        let tdir = tempdir().unwrap();

        // create the basic archive path
        let archive_dir = tdir.path();
        let _dir = create_dir(&archive_dir);
        let slurm_job_entry = SlurmJobEntry::new(&PathBuf::from("/tmp/some/job/path"), "1234");

        let p = Period::None;
        let target_path = determine_target_path(&archive_dir, &p, &slurm_job_entry, "foobar");

        assert_eq!(target_path, archive_dir.join(format!("job.1234_foobar")));

        let d = format!("{}", chrono::Local::now().format("%Y"));
        let p = Period::Yearly;
        let target_path = determine_target_path(&archive_dir, &p, &slurm_job_entry, "foobar");

        assert_eq!(target_path, archive_dir.join(d).join("job.1234_foobar"));

        let d = format!("{}", chrono::Local::now().format("%Y%m"));
        let p = Period::Monthly;
        let target_path = determine_target_path(&archive_dir, &p, &slurm_job_entry, "foobar");

        assert_eq!(target_path, archive_dir.join(d).join("job.1234_foobar"));

        let d = format!("{}", chrono::Local::now().format("%Y%m%d"));
        let p = Period::Daily;
        let target_path = determine_target_path(&archive_dir, &p, &slurm_job_entry, "foobar");

        assert_eq!(target_path, archive_dir.join(d).join("job.1234_foobar"));
    }

}