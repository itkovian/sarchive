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
use log::{debug, warn};
use std::collections::HashMap;
use std::fs;
use std::io::{Error, ErrorKind};
use std::path::{Path, PathBuf};
use std::thread::sleep;
use std::time::{Duration, Instant};

use super::job::JobInfo;
use super::Scheduler;

/// Representation of an entry in the Slurm job spool hash directories
pub struct SlurmJobEntry {
    /// The full path to the file that needs to be archived
    pub path_: PathBuf,
    /// The job ID
    jobid_: String,
    /// Time of event notification and instance creation
    moment_: Instant,
    /// The actual job script
    script_: Option<String>,
    /// The Slurm environment
    env_: Option<HashMap<String, String>>,
}

impl SlurmJobEntry {
    pub fn new(p: &PathBuf, id: &str) -> SlurmJobEntry {
        SlurmJobEntry {
            path_: p.clone(),
            jobid_: id.to_string(),
            moment_: Instant::now(),
            script_: None,
            env_: None,
        }
    }
}

fn read_file(path: &Path, filename: &str) -> Result<String, Error> {
    let fpath = path.join(filename);
    let mut iters = 100;
    let ten_millis = Duration::from_millis(10);
    while !Path::exists(&fpath) && iters > 0 {
        debug!("Waiting for {:?}", &fpath);
        sleep(ten_millis);
        if !Path::exists(&path) {
            debug!("Job directory {:?} no longer exists", &path);
            return Err(Error::new(
                ErrorKind::NotFound,
                format!("Job directory {:?} no longer exists", &path),
            ));
        }
        iters -= 1;
    }
    match iters {
        0 => {
            warn!("Timeout waiting for {:?} to appear", &fpath);
            Err(Error::new(
                ErrorKind::NotFound,
                format!("File {:?} did not appear after waiting 1s", &fpath),
            ))
        }
        _ => {
            let data = fs::read_to_string(&fpath)?;
            Ok(data)
        }
    }
}

impl JobInfo for SlurmJobEntry {
    fn jobid(&self) -> String {
        self.jobid_.clone()
    }

    fn moment(&self) -> Instant {
        self.moment_
    }

    fn read_job_info(&mut self) -> Result<(), Error> {
        self.script_ = Some(read_file(&self.path_, "script")?);

        let s = read_file(&self.path_, "environment")?;
        self.env_ = Some(
            s.split('\0')
                .filter(|s| !s.is_empty())
                .map(|s| {
                    let ps: Vec<_> = s.split('=').collect();
                    if ps.len() == 2 {
                        (ps[0].to_owned(), ps[1].to_owned())
                    } else {
                        (s.to_owned(), String::from(""))
                    }
                })
                .collect(),
        );
        Ok(())
    }

    fn files(&self) -> Vec<(String, String)> {
        if let Some(s) = &self.script_ {
            vec![
                (format!("job.{}_script", self.jobid_), s.to_string()),
                (
                    format!("job.{}_environment", self.jobid_),
                    format!("{:?}", self.env_),
                ),
            ]
        } else {
            Vec::new()
        }
    }

    fn script(&self) -> String {
        match &self.script_ {
            Some(s) => s.clone(),
            None => panic!("No script available for job {}", self.jobid_),
        }
    }

    fn extra_info(&self) -> Option<HashMap<String, String>> {
        self.env_.clone()
    }
}

pub struct Slurm {
    pub base: PathBuf,
}

impl Slurm {
    pub fn new(base: &PathBuf) -> Slurm {
        Slurm { base: base.clone() }
    }
}

impl Scheduler for Slurm {
    fn create_job_info(&self, event_path: &Path) -> Option<Box<dyn JobInfo>> {
        if let Some((jobid, _dirname)) = is_job_path(&event_path) {
            Some(Box::new(SlurmJobEntry::new(
                &event_path.to_path_buf(),
                jobid,
            )))
        } else {
            None
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
pub fn is_job_path(path: &Path) -> Option<(&str, &str)> {
    if path.is_dir() {
        let dirname = path.file_name().unwrap().to_str().unwrap();

        if dirname.starts_with("job.") {
            return Some((path.extension().unwrap().to_str().unwrap(), dirname));
        };
    }
    debug!("{:?} is not a considered job path", &path);
    None
}

#[cfg(test)]
mod tests {

    use super::*;
    use std::env::current_dir;
    use std::fs::create_dir;
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
    fn test_read_job_info() {
        let path = PathBuf::from(current_dir().unwrap().join("tests/job.123456"));
        let mut slurm_job_entry = SlurmJobEntry::new(&path, "123456");
        slurm_job_entry.read_job_info().unwrap();

        if let Some(hm) = slurm_job_entry.extra_info() {
            assert_eq!(hm.len(), 46);
            assert_eq!(hm.get("SLURM_CLUSTERS").unwrap(), "cluster");
            assert_eq!(hm.get("SLURM_NTASKS_PER_NODE").unwrap(), "1");
        } else {
            assert!(false);
        }
    }
}
