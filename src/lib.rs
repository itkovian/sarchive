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
#[macro_use]
extern crate slog;

use crossbeam_channel::{Receiver, Sender};
use notify::{DebouncedEvent, RecommendedWatcher, RecursiveMode, Watcher};
use std::fs::copy;
use std::io::Error;
use std::path::{Path, PathBuf};
use std::sync::mpsc::channel;
use std::time::Duration;

/// Representation of an entry in the Slurm job spool hash directories
pub struct SlurmJobEntry {
    /// The full path to the file that needs to be archived
    path: PathBuf,
    /// The job ID
    jobid: String,
    /// The filename in the job directory, this will be a suffix of the archived filename
    filename: String,
}

impl SlurmJobEntry {
    fn new(p: &PathBuf, id: &str, f: &str) -> SlurmJobEntry {
        SlurmJobEntry {
            path: p.clone(),
            jobid: id.to_string(),
            filename: f.to_string()
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
    if path.is_file() {
        let file = path.file_name().unwrap().to_str().unwrap();
        let parent = path.parent().unwrap();
        let jobname = parent.file_name().unwrap().to_str().unwrap();

        if jobname.starts_with("job.") {
            return Some((parent.extension().unwrap().to_str().unwrap(), file));
        };
    }
    None
}

/// Archives the file from the given SlurmJobEntry's path. 
fn archive(archive_path: &Path, slurm_job_entry: &SlurmJobEntry) -> Result<(), Error> {
    let target_path = archive.join_path(format!("job.{}_{}", &slurm_job_entry.jobid, &slurm_job_entry.filename));
    match copy(&slurm_job_entry.path, &target_path) {
        Ok(bytes) => info!("copied {} bytes from {:?} to {:?}", bytes, &slurm_job_entry.path, &target_path),
        Err(e) => {
            error!("Copy of {:?} to {:?} failed: {:?}", &slurm_job_entry.path, &target_path, e);
            return Err(e);
        }
    };
    Ok(())
}


fn check_and_queue(s: &Sender<SlurmJobEntry>, event: DebouncedEvent) -> Result<(), Error> {
    println!("Event received: {:?}", event);
    match event {
        DebouncedEvent::Create(path) | DebouncedEvent::Write(path) => {
            if let Some((jobid, job_filename)) = is_job_path(&path) {
                let e = SlurmJobEntry::new(&path, jobid, job_filename);
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
    watcher.watch(&path, RecursiveMode::Recursive)?;
    loop {
        match rx.recv() {
            Ok(event) => check_and_queue(s, event)?,
            Err(e) => {
                println!("Error on received event: {:?}", e);
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
                println!("Error on receiving SlurmJobEntry info");
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
        let file_path = jobdir.join("script");
        {
            let mut file = File::create(&file_path).unwrap();
            write!(file, "This is my jobscript").unwrap();
        }
        assert_eq!(is_job_path(&file_path), Some(("1234", "script")));

        // this should fail
        let fdir = tdir.path().join("fubar");
        let _faildir = create_dir(&fdir);
        let file_fail_path = fdir.join("script");
        {
            let mut file = File::create(&file_fail_path).unwrap();
            write!(file, "This is not a jobscript").unwrap();
        }
        assert_eq!(is_job_path(&file_fail_path), None);
    }
}
