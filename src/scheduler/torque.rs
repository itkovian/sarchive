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
use clap::Args;
use glob::glob;
use log::debug;
use notify::event::{CreateKind, Event, EventKind};
use std::collections::HashMap;
use std::io::Error;
use std::path::{Path, PathBuf};
use std::time::Instant;

use super::job::JobInfo;
use super::{Scheduler, SchedulerEvent};

use crate::utils;

#[derive(Args)]
pub struct TorqueArgs {
    subdirs: bool,
}

pub struct TorqueJobEntry {
    /// The full path to the file that needs to be archived
    /// This should be the `.SC` (script) file
    path_: PathBuf,
    /// The filename of the job script
    jobname_: Option<String>,
    /// The job ID
    jobid_: String,
    /// The name of the cluster
    cluster_: String,
    /// Time of event notification and instance creation
    moment_: Instant,
    /// The actual job script
    script_: Option<Vec<u8>>,
    /// Additional info for the job
    env_: HashMap<String, Vec<u8>>,
}

impl TorqueJobEntry {
    fn new(p: &Path, id: &str, cluster: &str) -> TorqueJobEntry {
        TorqueJobEntry {
            path_: p.to_path_buf(),
            jobname_: None,
            cluster_: cluster.to_string(),
            jobid_: id.to_owned(),
            moment_: Instant::now(),
            script_: None,
            env_: HashMap::new(),
        }
    }
}

impl JobInfo for TorqueJobEntry {
    fn jobid(&self) -> String {
        self.jobid_.clone()
    }

    // Return the moment of event occurence
    fn moment(&self) -> Instant {
        self.moment_
    }

    // Return the cluster to which the job was submitted
    fn cluster(&self) -> String {
        self.cluster_.clone()
    }

    // Retrieve all the information for the job from the spool location
    // This fills up the required data structures to be able to write
    // the backup or ship the information to some consumer
    fn read_job_info(&mut self) -> Result<(), Error> {
        let dir = self.path_.parent().unwrap();
        let filename = self.path_.strip_prefix(dir).unwrap();
        self.jobname_ = Some(filename.to_str().unwrap().to_string());
        self.script_ = Some(utils::read_file(dir, filename, None)?);

        // check for the presence of a .TA file
        let ta_filename = filename.with_extension("TA");
        let ta = utils::read_file(dir, &ta_filename, Some(10));
        if let Ok(ta_contents) = ta {
            self.env_
                .insert(ta_filename.to_str().unwrap().to_string(), ta_contents);
            // If the job is an array job, there are multiple JB files.
            // The file name pattern is: 2720868-946.master.cluster.JB
            // Split the filename into appropriate parts
            let fparts = filename.to_str().unwrap().split('.').collect::<Vec<&str>>();
            debug!(
                "Found TA file, looking for JB files in {:?} with name {}",
                dir, fparts[0]
            );
            glob(&format!("{}/{}-*.JB", dir.display(), fparts[0]))
                .unwrap()
                .filter_map(|jb_path| {
                    if let Ok(jb_path) = jb_path {
                        let jb_dir = jb_path.parent()?;
                        let jb_filename = jb_path.strip_prefix(jb_dir).unwrap();
                        let jb = utils::read_file(jb_dir, jb_filename, Some(10)).unwrap();
                        Some((jb_filename.to_owned(), jb))
                    } else {
                        None
                    }
                })
                .map(|(jb_filename, jb)| {
                    self.env_
                        .insert(jb_filename.to_str().unwrap().to_string(), jb);
                    Some(())
                })
                .for_each(drop);

            return Ok(());
        }

        // If it  was no array job, there should be a single .JB file to pick up.
        let jb_filename = filename.with_extension("JB");
        let jb = utils::read_file(dir, &jb_filename, None)?;
        self.env_
            .insert(jb_filename.to_str().unwrap().to_string(), jb);
        Ok(())
    }

    // Return a Vec of tuples with the filename and file contents for
    // each file that needs to be written as a backup
    fn files(&self) -> Vec<(String, Vec<u8>)> {
        let mut fs: Vec<(String, Vec<u8>)> = Vec::new();
        if let Some(jn) = &self.jobname_ {
            if let Some(script) = &self.script_ {
                fs.push((jn.to_string(), script.to_vec()));
            }
        }
        for (jb, jb_contents) in self.env_.iter() {
            fs.push((jb.to_string(), jb_contents.to_vec()));
        }
        fs
    }

    // Return the actual job script as a String
    fn script(&self) -> String {
        match &self.script_ {
            Some(s) => String::from_utf8_lossy(s).to_string(),
            None => panic!("No script available for job {}", self.jobid_),
        }
    }

    // Return additional information as a set of key-value pairs
    fn extra_info(&self) -> Option<HashMap<String, String>> {
        Some(
            self.env_
                .iter()
                .map(|(k, v)| (k.clone(), String::from_utf8_lossy(v).to_string()))
                .collect(),
        )
    }

    fn job_completion_info(&mut self) -> Result<(), Error> {
        Ok(())
    }

    fn extra_completion_info(&self) -> Option<HashMap<String, String>> {
        None
    }
}

pub struct Torque {
    pub base: PathBuf,
    pub cluster: String,
    pub subdirs: bool,
}

impl Torque {
    pub fn new(base: &Path, cluster: &str) -> Torque {
        Torque {
            base: base.to_path_buf(),
            cluster: cluster.to_string(),
            subdirs: true, // FIXME: get from the cli argument
        }
    }
}

impl Scheduler for Torque {
    fn watch_locations(&self) -> Vec<PathBuf> {
        if self.subdirs {
            (0..=9).map(|sd| self.base.join(format!("{sd}"))).collect()
        } else {
            [self.base.clone()].to_vec()
        }
    }

    fn construct_job_info(&self, event_path: &Path) -> Option<Box<dyn JobInfo>> {
        if let Some((jobid, filename)) = is_job_path(event_path) {
            Some(Box::new(TorqueJobEntry::new(
                filename,
                jobid,
                &self.cluster,
            )))
        } else {
            None
        }
    }

    // TODO: should we also check for deletion here?
    fn verify_event_kind(&self, event: &Event) -> Option<SchedulerEvent> {
        if let Event {
            kind: EventKind::Create(CreateKind::File),
            paths,
            ..
        } = event
        {
            Some(SchedulerEvent::Create(paths.to_vec()))
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
/// - the extension of the file is .SC (indicating a torque script file)
///
/// We return a tuple of two strings: the job ID and the filename, wrapped in
/// an Option.
fn is_job_path(path: &Path) -> Option<(&str, &Path)> {
    if path.is_file() {
        let jobid = path.file_stem().unwrap().to_str().unwrap();
        return match path.extension().unwrap().to_str().unwrap() {
            "SC" => Some((jobid, path)),
            _ => None,
        };
    }
    debug!("{:?} is not a considered job path", &path);
    None
}

#[cfg(test)]
mod tests {

    use super::*;
    use std::env::current_dir;

    #[test]
    fn test_read_info() {
        let path = PathBuf::from(
            current_dir()
                .unwrap()
                .join("tests/torque_job.1/1.mymaster.mycluster.SC"),
        );
        let mut torque_job_entry = TorqueJobEntry::new(&path, "1", "mycluster");
        torque_job_entry.read_job_info().unwrap();

        assert!(torque_job_entry
            .env_
            .contains_key("1.mymaster.mycluster.JB"));
        assert_eq!(
            torque_job_entry.env_.get("1.mymaster.mycluster.JB"),
            Some(&String::from("<some><xml>M</xml></some>").into_bytes())
        );
    }

    #[test]
    fn test_read_info_job_array() {
        let path = PathBuf::from(
            current_dir()
                .unwrap()
                .join("tests/torque_job.2/2.mymaster.mycluster.SC"),
        );
        let mut torque_job_entry = TorqueJobEntry::new(&path, "2", "mycluster");
        torque_job_entry.read_job_info().unwrap();

        assert!(torque_job_entry
            .env_
            .contains_key("2.mymaster.mycluster.TA"));
        assert!(torque_job_entry
            .env_
            .contains_key("2-1.mymaster.mycluster.JB"));
        assert!(torque_job_entry
            .env_
            .contains_key("2-2.mymaster.mycluster.JB"));
        assert_eq!(
            torque_job_entry.env_.get("2-1.mymaster.mycluster.JB"),
            Some(&String::from("<some><xml>M1</xml></some>").into_bytes())
        );
        assert_eq!(
            torque_job_entry.env_.get("2-2.mymaster.mycluster.JB"),
            Some(&String::from("<some><xml>M2</xml></some>").into_bytes())
        );
    }
}
