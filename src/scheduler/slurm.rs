/*
Copyright 2019-2024 Andy Georges <itkovian+sarchive@gmail.com>

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
use log::debug;
use notify::event::{CreateKind, Event, EventKind};
use regex::Regex;
use std::collections::HashMap;
use std::io::Error;
use std::path::{Path, PathBuf};
use std::string::String;
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
    script_: Option<Vec<u8>>,
    /// The job's environment in Slurm
    env_: Option<Vec<u8>>,
    /// Filter for the environment
    filter_regex: Option<Regex>,
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
    /// let job_entry = SlurmJobEntry::new(&p, &id, &cluster, &None);
    ///
    /// assert_eq!(job_entry.path_, p);
    /// ```
    pub fn new(
        path: &Path,
        id: &str,
        cluster: &str,
        filter_regex: &Option<Regex>,
    ) -> SlurmJobEntry {
        SlurmJobEntry {
            path_: path.to_path_buf(),
            jobid_: id.to_string(),
            cluster_: cluster.to_string(),
            moment_: Instant::now(),
            script_: None,
            env_: None,
            filter_regex: filter_regex.clone(),
        }
    }
}

fn filter_env(r: &Option<Regex>, env: &str) -> bool {
    if let Some(rs) = r {
        if rs.is_match(env) {
            return true;
        }
    }
    false
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
        self.script_ = {
            let mut s = utils::read_file(&self.path_, Path::new("script"), None)?;
            if let Some(0) = s.last() {
                s.pop();
            }
            Some(s)
        };
        self.env_ = Some(utils::read_file(
            &self.path_,
            Path::new("environment"),
            None,
        )?);
        Ok(())
    }

    /// Returns a `Vector` with tuples containing the filename and the
    /// file contents for the script and environment files
    fn files(&self) -> Vec<(String, Vec<u8>)> {
        [
            ("script", self.script_.as_ref()),
            ("environment", self.env_.as_ref()),
        ]
        .iter()
        .filter_map(|(filename, v)| {
            v.map(|s| (format!("job.{}_{}", self.jobid_, filename), s.to_owned()))
        })
        .collect()
    }

    /// Returns the job script as a `String`
    fn script(&self) -> String {
        match &self.script_ {
            Some(s) => String::from_utf8_lossy(s).to_string(),
            None => panic!("No script available for job {}", self.jobid_),
        }
    }

    /// Returns the environment info (if any) as a HashMap, mapping env keys
    /// to values
    fn extra_info(&self) -> Option<HashMap<String, String>> {
        let r = self.filter_regex.clone();
        self.env_.as_ref().map(|s| {
            let env_string = String::from_utf8_lossy(s.split_at(4).1).to_string();
            env_string
                .split('\0')
                .filter_map(|entry| {
                    let entry = entry.trim();
                    if !entry.is_empty() {
                        let parts: Vec<_> = entry.split('=').collect();
                        match parts.len() {
                            2 => {
                                let key = parts[0].trim();
                                println!("Checking for key {}", &key);
                                if !key.is_empty() && !filter_env(&r, key) {
                                    println!("Keeping key {}", &key);
                                    Some((key.to_owned(), parts[1].to_owned()))
                                } else {
                                    None
                                }
                            }
                            _ => Some((entry.to_owned(), String::from(""))),
                        }
                    } else {
                        None
                    }
                })
                .collect::<HashMap<String, String>>()
        })
    }
}

/// Representation of the Slurm scheduler
pub struct Slurm {
    /// The absolute path to the spool directory
    pub base: PathBuf,
    pub cluster: String,
    pub filter_regex: Option<Regex>,
}

impl Slurm {
    /// Returns a new `Slurm` with the specified base path, cluster, and arguments.
    ///
    /// # Arguments
    ///
    /// * `base` - A reference to a `Path` representing the base path.
    /// * `cluster` - A string slice representing the name of the cluster.
    /// * `args` - A reference to `SlurmArgs` containing additional arguments.
    ///
    /// # Example
    ///
    /// ```
    /// # use regex::Regex;
    /// # use std::path::PathBuf;
    /// # use sarchive::scheduler::slurm::{Slurm};
    ///
    /// let base = PathBuf::from("/var/spool/slurm/hash.3/5678");
    ///
    /// let slurm = Slurm::new(&base, "mycluster", &Regex::new(".*").ok());
    ///
    /// assert_eq!(slurm.base, base);
    /// assert_eq!(slurm.cluster, "mycluster");
    /// ```
    ///
    pub fn new(base: &Path, cluster: &str, filter_regex: &Option<Regex>) -> Slurm {
        Slurm {
            base: base.to_path_buf(),
            cluster: cluster.to_string(),
            filter_regex: filter_regex.clone(),
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
    fn watch_locations(&self) -> Vec<PathBuf> {
        (0..=9)
            .map(|hash| self.base.join(format!("hash.{hash}")))
            .collect()
    }

    /// Returns a Box wrapping the actual job info data structure.App
    ///
    /// # Arguments
    ///
    /// * event_path: A `Path to the job directory that
    fn create_job_info(&self, event_path: &Path) -> Option<Box<dyn JobInfo>> {
        if let Some((jobid, _dirname)) = is_job_path(event_path) {
            Some(Box::new(SlurmJobEntry::new(
                event_path,
                jobid,
                &self.cluster,
                &self.filter_regex,
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
    fn test_read_job_script_drop_zero() {
        let path = PathBuf::from(current_dir().unwrap().join("tests/job.123456"));
        let mut slurm_job_entry = SlurmJobEntry::new(&path, "123456", "mycluster", &None);
        slurm_job_entry.read_job_info().unwrap();

        // check the script
        let s = slurm_job_entry;
        assert!(s.script_.unwrap().last() != Some(&0));
    }

    #[test]
    fn test_read_job_extra_info() {
        let path = PathBuf::from(current_dir().unwrap().join("tests/job.123456"));
        let mut slurm_job_entry = SlurmJobEntry::new(&path, "123456", "mycluster", &None);
        slurm_job_entry.read_job_info().unwrap();

        // check the environment information
        if let Some(hm) = slurm_job_entry.extra_info() {
            println!("hm length: {}", hm.len());
            assert_eq!(hm.len(), 45);
            assert_eq!(hm.get("SLURM_CLUSTERS").unwrap(), "cluster");
            assert_eq!(hm.get("SLURM_NTASKS_PER_NODE").unwrap(), "1");
        } else {
            assert!(false);
        }
    }

    #[test]
    fn test_extra_info_drop_u32_prefix() {
        let path = PathBuf::from(current_dir().unwrap().join("tests/job.8897161"));
        let mut slurm_job_entry = SlurmJobEntry::new(&path, "8897161", "mycluster", &None);
        if let Err(e) = slurm_job_entry.read_job_info() {
            println!("Could not read job info: {:?}", e);
            assert!(false);
        }

        if let Some(_hm) = slurm_job_entry.extra_info() {
        } else {
            assert!(false);
        }
    }

    #[test]
    fn test_extra_info() {
        let env_data = b"\0\0\0\0VAR1=value1\0VAR2=value2\0VAR3=value3\0";
        let filter_regex = Regex::new("VAR[12]").ok();

        let job_entry = SlurmJobEntry {
            path_: PathBuf::from("/some/path"),
            jobid_: "12345".to_string(),
            cluster_: "mycluster".to_string(),
            moment_: Instant::now(),
            script_: None,
            env_: Some(env_data.to_vec()),
            filter_regex,
        };

        let extra_info = job_entry.extra_info().unwrap();

        assert_eq!(extra_info.get("VAR1"), None);
        assert_eq!(extra_info.get("VAR2"), None);
        assert_eq!(extra_info.get("VAR3"), Some(&"value3".to_string()));
    }

    #[test]
    fn test_filter_env() {
        let regex = Regex::new("VAR.*").ok();
        assert!(filter_env(&regex, "VAR1"));
        assert!(filter_env(&regex, "VAR2"));
        assert!(!filter_env(&regex, "OTHER"));
    }

    #[test]
    fn test_filter_env_none() {
        let regex = None;
        assert!(!filter_env(&regex, "VAR1"));
        assert!(!filter_env(&regex, "VAR2"));
        assert!(!filter_env(&regex, "OTHER"));
    }
}
