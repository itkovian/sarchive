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
use log::*;
use notify::{Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use std::fs::{copy, create_dir_all};
use std::io::Error;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::sleep;
use std::time::Duration;

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

/// Determines the target path for the slurm job file
///
/// The path will have the following components:
/// - the archive path
/// - a subdir depending on the Period
///     - YYYY in case of a Yearly Period
///     - YYYYMM in case of a Monthly Period
///     - YYYYMMDD in case of a Daily Period 
/// - a file with the given filename
fn determine_target_path(archive_path: &Path, p: &Period, slurm_job_entry: &SlurmJobEntry, filename: &str) -> PathBuf {
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
            archive_subdir_path.clone().join(format!("job.{}_{}", &slurm_job_entry.jobid, &filename))
        },
        None => archive_path.join(format!("job.{}_{}", &slurm_job_entry.jobid, &filename))
    }
}

/// Archives the files from the given SlurmJobEntry's path.
/// 
/// We busy wait for 1 second, sleeping for 10 ms per turn for
/// the environment and script files to appear.
/// If the files cannot be found after that tine, we output a warning
/// and return without copying. 
/// If the directory dissapears before we found or copied the files, 
/// we panic.
fn archive(archive_path: &Path, p: &Period, slurm_job_entry: &SlurmJobEntry) -> Result<(), Error> {
    // We wait for each file to be present
    let ten_millis = Duration::from_millis(10);
    for filename in &["script", "environment"] {
        let fpath = slurm_job_entry.path.join(filename);
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

        let target_path =  determine_target_path(&archive_path, &p, &slurm_job_entry, &filename);
        
        match copy(&fpath, &target_path) {
            Ok(bytes) => info!(
                "copied {} bytes from {:?} to {:?}",
                bytes, &fpath, &target_path
            ),
            Err(e) => {
                error!(
                    "Copy of {:?} to {:?} failed: {:?}",
                    &slurm_job_entry.path, &target_path, e
                );
                return Err(e);
            }
        };
    }

    Ok(())
}

fn check_and_queue(s: &Sender<SlurmJobEntry>, event: Event) -> Result<(), Error> {
    debug!("Event received: {:?}", event);
    match event.kind {
        EventKind::Create(Any) => {
            if let Some((jobid, _dirname)) = is_job_path(&event.paths[0]) {
                let e = SlurmJobEntry::new(&event.paths[0], jobid);
                s.send(e).unwrap();
            };
        }
        // We ignore all other events
        _ => (),
    }
    Ok(())
}

pub fn monitor(base: &Path, hash: u8, s: &Sender<SlurmJobEntry>, sigchannel: &Receiver<bool>) -> notify::Result<()> {
    let (tx, rx) = unbounded();

    // create a platform-specific watcher
    let mut watcher: RecommendedWatcher = Watcher::new(tx, Duration::from_secs(2))?;
    let path = base.join(format!("hash.{}", hash));

    info!("Watching path {:?}", &path);

    if let Err(e) = watcher.watch(&path, RecursiveMode::NonRecursive) { return Err(e); } 
    loop {
        select! {
            recv(sigchannel) -> b => if let Ok(true) = b  {
                return Ok(());
            },
            recv(rx) -> event => { match event {
                Ok(e) => check_and_queue(s, e.unwrap())?,
                Err(e) => {
                    error!("Error on received event: {:?}", e);
                    break;
                }
            };}
        }
    };

    Ok(())
}

pub fn process(archive_path: &Path, p: Period, r: &Receiver<SlurmJobEntry>, sigchannel: &Receiver<bool>) {

    info!("Start processing events");
    loop {
        select! {
            recv(sigchannel) -> b => if let Ok(true) = b  {
                return;
            },
            recv(r) -> entry => { match entry {
                Ok(slurm_job_entry) => archive(&archive_path, &p, &slurm_job_entry),
                Err(_) => {
                    error!("Error on receiving SlurmJobEntry info");
                    break;
                }
            };}
        }
    };
}

#[cfg(test)]
mod tests {

    extern crate tempfile;

    use super::*;
    use std::fs::{create_dir, read_to_string, File};
    use std::io::Write;
    use std::path::Path;
    use tempfile::{tempdir};

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

        let archive_env_contents = read_to_string(&archive_dir.join("job.1234_environment")).unwrap();
        assert_eq!(&archive_env_contents, "environment");

        let archive_script_contents = read_to_string(&archive_dir.join("job.1234_script")).unwrap();
        assert_eq!(&archive_script_contents, "job script");
    }
}
