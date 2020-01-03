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
use clap::ArgMatches;
use log::debug;
use notify::event::{CreateKind, Event, EventKind};
use std::collections::HashMap;
use std::io::Error;
use std::path::{Path, PathBuf};
use std::time::Instant;

use super::job::JobInfo;
use super::Scheduler;
use crate::utils;

/// Representation of an entry in the Slurm job spool hash directories
pub struct SlurmJobEntry {
    /// The full path to the job information directory
    pub path_: PathBuf,
    /// The job ID
    jobid_: String,
    /// The name of the cluster
    cluster_: String,
    /// Time of event notification and instance creation
    moment_: Instant,
    /// The actual job script
    script_: Option<String>,
    /// The job's environment in Slurm
    env_: Option<String>,
}

impl SlurmJobEntry {
    /// Returns a new SlurmJobEntry with the given path to the job info and the given job ID
    ///
    /// # Arguments
    ///
    /// * `path` - A `PathBuf` pointing to the directory (usually .../job.<jobid>)
    /// * `id` - A string slice representing the job ID
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::path::{PathBuf};
    /// # use sarchive::scheduler::slurm::SlurmJobEntry;
    ///
    /// let p = PathBuf::from("/var/spool/slurm/hash.2/job.1234");
    /// let id = "1234";
    /// let cluster = "mycluster";
    ///
    /// let job_entry = SlurmJobEntry::new(&p, &id, &cluster);
    ///
    /// assert_eq!(job_entry.path_, p);
    /// ```
    pub fn new(path: &PathBuf, id: &str, cluster: &str) -> SlurmJobEntry {
        SlurmJobEntry {
            path_: path.clone(),
            jobid_: id.to_string(),
            cluster_: cluster.to_string(),
            moment_: Instant::now(),
            script_: None,
            env_: None,
        }
    }
}

impl JobInfo for SlurmJobEntry {
    /// Returns the job ID as a `String`
    fn jobid(&self) -> String {
        self.jobid_.clone()
    }

    /// Returns the time the jpb entry was created by SArchive
    fn moment(&self) -> Instant {
        self.moment_
    }

    // Return the cluster to which the job was submitted
    fn cluster(&self) -> String {
        self.cluster_.clone()
    }

    /// Populates the job entry structure with the relevant information
    ///
    /// For Slurm, this encompasses the job script and the job environment
    fn read_job_info(&mut self) -> Result<(), Error> {
        self.script_ = Some(utils::read_file(&self.path_, &Path::new("script"))?);
        self.env_ = Some(utils::read_file(&self.path_, &Path::new("environment"))?);
        Ok(())
    }

    /// Returns a `Vector` with tuples containing the filename and the
    /// file contents for the script and environment files
    fn files(&self) -> Vec<(String, String)> {
        [
            ("script", self.script_.as_ref()),
            ("environment", self.env_.as_ref()),
        ]
        .iter()
        .filter_map(|(filename, v)| {
            v.map(|s| (format!("job.{}_{}", self.jobid_, filename), s.to_string()))
        })
        .collect()
    }

    /// Returns the job script as a `String`
    fn script(&self) -> String {
        match &self.script_ {
            Some(s) => s.clone(),
            None => panic!("No script available for job {}", self.jobid_),
        }
    }

    /// Returns the environment info (if any) as a HashMap, mapping env keys
    /// to values
    fn extra_info(&self) -> Option<HashMap<String, String>> {
        self.env_.as_ref().map(|s| {
            s.split('\0')
                .filter_map(|s| {
                    let s = s.trim();
                    if !s.is_empty() {
                        let ps: Vec<_> = s.split('=').collect();
                        match ps.len() {
                            2 => {
                                if !ps[0].trim().is_empty() {
                                    Some((ps[0].to_owned(), ps[1].to_owned()))
                                } else {
                                    None
                                }
                            }
                            _ => Some((s.to_owned(), String::from(""))),
                        }
                    } else {
                        None
                    }
                })
                .collect()
        })
    }
}

/// Representation of the Slurm scheduler
pub struct Slurm {
    /// The absolute path to the spool directory
    pub base: PathBuf,
    pub cluster: String,
}

impl Slurm {
    /// Returns a new Slurm with the base path set.
    ///
    /// # Arguments
    ///
    /// * `base`: a PathBuf reference
    ///
    /// # Example
    ///
    /// ```
    /// # use std::path::PathBuf;
    /// # use sarchive::scheduler::slurm::Slurm;
    ///
    /// let base = PathBuf::from("/var/spool/slurm/hash.3/5678");
    ///
    /// let slurm = Slurm::new(&base, "mycluster");
    ///
    /// assert_eq!(slurm.base, base);
    /// ```
    pub fn new(base: &PathBuf, cluster: &str) -> Slurm {
        Slurm {
            base: base.clone(),
            cluster: cluster.to_string(),
        }
    }
}

impl Scheduler for Slurm {
    /// Return a `Vector` with the locations that need to be watched.
    ///
    /// The is the base path + hash.{0..9}
    ///
    /// # Arguments
    ///
    /// * _matches: reference the ArgMatches in case we pass command line
    ///             options, which is not done atm.
    fn watch_locations(&self, _matches: &ArgMatches) -> Vec<PathBuf> {
        (0..=9)
            .map(|hash| self.base.join(format!("hash.{}", hash)).to_owned())
            .collect()
    }

    /// Returns a Box wrapping the actual job info data structure.App
    ///
    /// # Arguments
    ///
    /// * event_path: A `Path to the job directory that
    fn create_job_info(&self, event_path: &Path) -> Option<Box<dyn JobInfo>> {
        if let Some((jobid, _dirname)) = is_job_path(&event_path) {
            Some(Box::new(SlurmJobEntry::new(
                &event_path.to_path_buf(),
                jobid,
                &self.cluster,
            )))
        } else {
            None
        }
    }

    fn verify_event_kind(&self, event: &Event) -> Option<Vec<PathBuf>> {
        if let Event {
            kind: EventKind::Create(CreateKind::Folder),
            paths,
            ..
        } = event
        {
            Some(paths.to_vec())
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
        let mut slurm_job_entry = SlurmJobEntry::new(&path, "123456", "mycluster");
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
