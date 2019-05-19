extern crate crossbeam_channel;
extern crate crossbeam_queue;
extern crate crossbeam_utils;

use crossbeam_channel::{Receiver, Sender};
use crossbeam_queue::SegQueue;
use notify::{DebouncedEvent, RecommendedWatcher, RecursiveMode, Watcher};
use std::fs::copy;
use std::io::Error;
use std::path::{Path, PathBuf};
use std::sync::mpsc::channel;
use std::time::Duration;


pub struct SlurmJobEntry {
    path: PathBuf,
    jobid: String,
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


fn is_job_path(path: &Path) -> Option<(&str, &str)> {
    // e.g., /var/spool/slurm/hash.1/job.0021/script
    // what we need is the job ID (0021) and the name of the actual file (script)
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


fn archive(archive: &Path, j: &SlurmJobEntry) -> Result<(), Error> {
    let target_path = archive.join(format!("job.{}_{}", &j.jobid, &j.filename));
    match copy(&j.path, &target_path) {
        Ok(bytes) => println!("{} bytes copied to {:?}", bytes, &target_path),
        Err(e) => {
            println!("Copy of {:?} to {:?} failed: {:?}", &j.path, &target_path, e);
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
