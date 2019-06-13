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

use crossbeam_channel::{select, unbounded, Receiver, Sender};
use crossbeam_utils::sync::Parker;
use crossbeam_utils::Backoff;
use log::*;
use notify::{Op, RawEvent, Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use std::fs::{copy, create_dir_all};
use std::io::Error;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::Arc;
use std::time::Duration;

/// Representation of an entry in the Slurm job spool hash directories
pub struct TorqueJobEntry    {
    /// The full path to the file that needs to be archived
    path: PathBuf,
    /// The job ID
    jobid: String,
    /// The file type
    file_type: String,
}

impl TorqueJobEntry {
    fn new(p: &PathBuf, id: &str, t: &str) -> TorqueJobEntry {
        TorqueJobEntry {
            path: p.clone(),
            jobid: id.to_owned(),
            file_type: t.to_owned(),
        }
    }
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
fn is_job_path(path: &Path) -> Option<(&str, &Path, String)> {
    if path.is_file() {
        let jobid = path.file_stem().unwrap().to_str().unwrap();
        let file_type = path.extension().unwrap().to_str().unwrap();
        return Some((jobid, path, file_type.to_string()));
    }
    debug!("{:?} is not a considered job path", &path);
    None
}

/// Determines the target path for the slurm job file
///
/// The path will have the following components:
/// - the archive path
/// - a subdir depending on the Period
///     - YYYY in case of a Yearly Period
///     - YYYYMM in case of a Monthly Period
///     - YYYYMMDD in case of a Daily Period
/// - a file with the given filename
fn determine_target_path(archive_path: &Path, p: &Period, job_entry: &TorqueJobEntry, filename: &str) -> PathBuf {
    let archive_subdir = match p {
        Period::Yearly => Some(format!("{}", chrono::Local::now().format("%Y"))),
        Period::Monthly => Some(format!("{}", chrono::Local::now().format("%Y%m"))),
        Period::Daily => Some(format!("{}", chrono::Local::now().format("%Y%m%d"))),
        _ => None,
    };
    debug!("Archive subdir is {:?}", &archive_subdir);
    match archive_subdir {
        Some(d) => {
            let archive_subdir_path = archive_path.join(&d);
            if !Path::exists(&archive_subdir_path) {
                debug!("Archive subdir {:?} does not yet exist, creating", &d);
                create_dir_all(&archive_subdir_path).unwrap();
            }
            archive_subdir_path.clone().join(format!("job.{}_{}", &job_entry.jobid, &filename))
        },
        None => archive_path.join(format!("job.{}_{}", &job_entry.jobid, &filename))
    }
}

/// Archives the files from the given TorqueJobEntry's path.
/// 
/// We busy wait for 1 second, sleeping for 10 ms per turn for
/// the environment and script files to appear.
/// If the files cannot be found after that tine, we output a warning
/// and return without copying.
/// If the directory dissapears before we found or copied the files,
/// we panic.
fn archive(archive_path: &Path, p: &Period, job_entry: &TorqueJobEntry) -> Result<(), Error> {
    // We wait for each file to be present
    let fpath = &job_entry.path;
    let target_path =  determine_target_path(&archive_path, &p, &job_entry, &job_entry.file_type);
        
    match copy(&fpath, &target_path) {
        Ok(bytes) => info!(
            "copied {} bytes from {:?} to {:?}",
            bytes, &fpath, &target_path
        ),
        Err(e) => {
            error!(
                "Copy of {:?} to {:?} failed: {:?}",
                &job_entry.path, &target_path, e
            );
            return Err(e);
        }
    };
    Ok(())
}

fn check_and_queue(s: &Sender<TorqueJobEntry>, event: RawEvent) -> Result<(), Error> {
    debug!("Event received: {:?}", event);
    match event {
        RawEvent{path: Some(path), op: Ok(Op::CLOSE_WRITE), cookie} => {
            if let Some((jobid, _dirname, file_type)) = is_job_path(&path) {
                let e = TorqueJobEntry::new(&path, jobid, &file_type);
                s.send(e).unwrap();
            };
        }
        // We ignore all other events
        _ => (),
    }
    Ok(())
}

pub fn monitor(path: &Path, s: &Sender<TorqueJobEntry>, sigchannel: &Receiver<bool>) -> notify::Result<()> {
    let (tx, rx) = unbounded();

    // create a platform-specific watcher
    let mut watcher: RecommendedWatcher = Watcher::new_immediate(tx)?;

    info!("Watching path {:?}", &path);

    if let Err(e) = watcher.watch(&path, RecursiveMode::NonRecursive) {
        return Err(e);
    }
    loop {
        select! {
            recv(sigchannel) -> b => if let Ok(true) = b  {
                return Ok(());
            },
            recv(rx) -> event => { match event {
                Ok(e) => check_and_queue(s, e)?,
                Err(e) => {
                    error!("Error on received event: {:?}", e);
                    break;
                }
            };}
        }
    }

    Ok(())
}

/// The process function consumes job entries and call the archive function for each
/// received entry.
/// At the same time, it also checks if there is an incoming notification that it should
/// stop processing. Upon receipt, it will cease operations immediately.
pub fn process(
    archive_path: &Path,
    p: Period,
    r: &Receiver<TorqueJobEntry>,
    sigchannel: &Receiver<bool>,
    cleanup: bool,
) {
    info!("Start processing events");
    loop {
        select! {
            recv(sigchannel) -> b => if let Ok(true) = b  {
                if !cleanup {
                    info!("Stopped processing entries, {} skipped", r.len());
                } else {
                info!("Processing {} entries, then stopping", r.len());
                r.iter().map(|entry| archive(&archive_path, &p, &entry).unwrap());
                info!("Done processing");
                }
                return;
            },
            recv(r) -> entry => { match entry {
                Ok(job_entry) => archive(&archive_path, &p, &job_entry),
                Err(_) => {
                    error!("Error on receiving TorqueJobEntry info");
                    break;
                }
            };}
        }
    }
    debug!("Processing should never get here")
}

/// This function will park the thread until it is unparked and check the
/// atomic bool to see if it should start notifying other threads they need
/// to finish execution.
pub fn signal_handler_atomic(sender: &Sender<bool>, sig: Arc<AtomicBool>, p: &Parker) {
    let backoff = Backoff::new();
    while !sig.load(SeqCst) {
        if backoff.is_completed() {
            p.park();
        } else {
            backoff.snooze();
        }
    }
    for _ in 0..20 {
        sender.send(true);
    }
    info!("Sent 20 notifications");
}

#[cfg(test)]
mod tests {

    extern crate tempfile;

    use super::*;
    use std::fs::{create_dir, read_to_string, File};
    use std::io::Write;
    use std::path::Path;
    use tempfile::tempdir;

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

    #[test]
    fn test_determine_target_path() {
        let tdir = tempdir().unwrap();

        // create the basic archive path
        let archive_dir = tdir.path();
        let _dir = create_dir(&archive_dir);
        let slurm_job_entry = TorqueJobEntry::new(&PathBuf::from("/tmp/some/job/path"), "1234");

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

    #[test]
    fn test_archive() {
        let tdir = tempdir().unwrap();

        // create the basic archive path
        let archive_dir = tdir.path().join("archive");
        let _dir = create_dir(&archive_dir);

        // create the basic job path
        let job_dir = tdir.path().join("job.1234");
        let _dir = create_dir(&job_dir);

        // create env and script files
        let env_path = job_dir.join("environment");
        let mut env = File::create(env_path).unwrap();
        env.write(b"environment");

        let job_path = job_dir.join("script");
        let mut job = File::create(&job_path).unwrap();
        job.write(b"job script");

        let slurm_job_entry = SlurmJobEntry::new(&job_dir, "1234");

        archive(&archive_dir, &Period::None, &slurm_job_entry);

        assert!(Path::is_file(&archive_dir.join("job.1234_environment")));
        assert!(Path::is_file(&archive_dir.join("job.1234_script")));

        let archive_env_contents =
            read_to_string(&archive_dir.join("job.1234_environment")).unwrap();
        assert_eq!(&archive_env_contents, "environment");

        let archive_script_contents = read_to_string(&archive_dir.join("job.1234_script")).unwrap();
        assert_eq!(&archive_script_contents, "job script");
    }
}
