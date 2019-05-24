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
extern crate crossbeam_channel;
extern crate crossbeam_utils;


use crossbeam_channel::{Receiver, Sender};
use notify::{DebouncedEvent, RecommendedWatcher, RecursiveMode, Watcher};
use log::*;
use std::fs::copy;
use std::io::Error;
use std::path::{Path, PathBuf};
use std::sync::mpsc::channel;
use std::time::Duration;
use std::thread::sleep;

/// Representation of an entry in the Slurm job spool hash directories
pub struct SlurmJobEntry {
    /// The full path to the file that needs to be archived
    path: PathBuf,
    /// The job ID
    jobid: String,
}

impl SlurmJobEntry {
    fn new(p: &PathBuf, id: &str) -> SlurmJobEntry {
        SlurmJobEntry {
            path: p.clone(),
            jobid: id.to_string(),
        }
    } 
}

/// Verifies that the path metioned in the event is a that of a file that
/// needs archival
///
/// This ignores the path prefix, but verifies that 
/// - the path points to a file
/// - there is a path dir component that starts with "job."
/// 
/// For example, /var/spool/slurm/hash.3/job.01234./script is a valid path
///
/// We return a tuple of two strings: the job ID and the filename, wrapped in
/// an Option.
fn is_job_path(path: &Path) -> Option<(&str, &str)> {
    if path.is_dir() {
        let dirname = path.file_name().unwrap().to_str().unwrap();

        if dirname.starts_with("job.") {
            return Some((path.extension().unwrap().to_str().unwrap(), dirname));
        };
    }
    debug!("{:?} is not a considered job path", &path);
    None
}

/// Archives the files from the given SlurmJobEntry's path. 
fn archive(archive_path: &Path, slurm_job_entry: &SlurmJobEntry) -> Result<(), Error> {

    // We wait for each file to be present
    let ten_millis = Duration::from_millis(10);
    for filename in vec!["script", "environment"] {

        let fpath = slurm_job_entry.path.join("script");
        let mut iters = 100;
        while !Path::exists(&fpath) && iters > 0 { 
            debug!("Waiting for {:?}", fpath);
            sleep(ten_millis);
            if !Path::exists(&slurm_job_entry.path) {
                error!("Job directory {:?} no longer exists", &slurm_job_entry.path);
                panic!("path not found");
            }
            iters -= 1;
        }
        if iters == 0 {
            warn!("Cannot make copy of {:?}", fpath);
            continue;
        }

        let target_path = archive_path.join(format!("job.{}_{}", &slurm_job_entry.jobid, &filename));
        match copy(&fpath, &target_path) {
            Ok(bytes) => info!("copied {} bytes from {:?} to {:?}", bytes, &fpath, &target_path),
            Err(e) => {
                error!("Copy of {:?} to {:?} failed: {:?}", &slurm_job_entry.path, &target_path, e);
                return Err(e);
            }
        };
    };
    
    Ok(())
}


fn check_and_queue(s: &Sender<SlurmJobEntry>, event: DebouncedEvent) -> Result<(), Error> {
    debug!("Event received: {:?}", event);
    match event {
        DebouncedEvent::Create(path) | DebouncedEvent::Write(path) => {
            if let Some((jobid, dirname)) = is_job_path(&path) {
                let e = SlurmJobEntry::new(&path, jobid);
                s.send(e).unwrap();
            };
        }
        // We ignore all other events
        _ => (),
    }
    Ok(())
}

pub fn monitor(base: &Path, hash: u8, s: &Sender<SlurmJobEntry>) -> notify::Result<()> {
    let (tx, rx) = channel();

    // create a platform-specific watcher
    let mut watcher: RecommendedWatcher = Watcher::new(tx, Duration::from_secs(2))?;
    let path = base.join(format!("hash.{}", hash));

    // TODO: check the path exists!
    watcher.watch(&path, RecursiveMode::NonRecursive);
    loop {
        match rx.recv() {
            Ok(event) => check_and_queue(s, event)?,
            Err(e) => {
                error!("Error on received event: {:?}", e);
                break;
            }
        };
    }

    Ok(())
}

pub fn process(archive_path: &Path, r: &Receiver<SlurmJobEntry>) {

    loop {
        match r.recv() {
            Ok(slurm_job_entry) => archive(&archive_path, &slurm_job_entry),
            Err(e) => {
                error!("Error on receiving SlurmJobEntry info");
                Ok(())
            }
        };
    }
}

#[cfg(test)]
mod tests {

    extern crate tempfile;

    use super::*;
    use std::fs::{create_dir, remove_dir, File};
    use std::io::Write;
    use std::path::Path;
    use tempfile::{tempdir, tempfile};

    #[test]
    fn test_is_job_path() {
        let tdir = tempdir().unwrap();

        // this should pass
        let jobdir = tdir.path().join("job.1234");
        let _dir = create_dir(&jobdir);
        assert_eq!(is_job_path(&jobdir), Some(("1234", "job.1234")));

        // this should fail
        let fdir = tdir.path().join("fubar");
        let _faildir = create_dir(&fdir);
        assert_eq!(is_job_path(&fdir), None);
    }
}
