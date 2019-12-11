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
use glob::glob;
use log::debug;
use notify::event::{CreateKind, Event, EventKind};
use std::collections::HashMap;
use std::io::Error;
use std::path::{Path, PathBuf};
use std::time::Instant;

use super::job::JobInfo;
use super::Scheduler;

use crate::utils;

pub struct TorqueJobEntry {
    /// The full path to the file that needs to be archived
    path_: PathBuf,
    /// The filename of the job
    jobname_: Option<String>,
    /// The job ID
    jobid_: String,
    /// Time of event notification and instance creation
    moment_: Instant,
    /// The actual job script
    script_: Option<String>,
    /// Additional info for the job
    env_: HashMap<String, String>,
}

impl TorqueJobEntry {
    fn new(p: &PathBuf, id: &str) -> TorqueJobEntry {
        TorqueJobEntry {
            path_: p.clone(),
            jobname_: None,
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

    // Retrieve all the information for the job from the spool location
    // This fills up the required data structures to be able to write
    // the backup or ship the information to some consumer
    fn read_job_info(&mut self) -> Result<(), Error> {
        let dir = self.path_.parent().unwrap();
        let filename = self.path_.strip_prefix(&dir).unwrap();
        self.jobname_ = Some(filename.to_str().unwrap().to_string());
        self.script_ = Some(utils::read_file(&dir, &filename)?);

        // check for the presence of a .TA file
        if self.path_.with_extension("TA").exists() {
            // First, get the array file
            let ta_filename = filename.with_extension("TA");
            let ta = utils::read_file(dir, &ta_filename)?;
            self.env_
                .insert(ta_filename.to_str().unwrap().to_string(), ta.to_owned());
            // If the job is an array job, there are multiple JB files.
            // The file name pattern is: 2720868-946.master.cluster.JB
            // Split the filename into appropriate parts
            let fparts = filename.to_str().unwrap().split(".").collect::<Vec<&str>>();
            glob(&format!("{:?}/{}-*.JB", dir, fparts[0]))
                .unwrap()
                .filter_map(|jb_path| {
                    if let Ok(jb_path) = jb_path {
                        let jb_dir = jb_path.parent()?;
                        let jb_filename = jb_path.strip_prefix(&jb_dir).unwrap();
                        let jb = utils::read_file(&jb_dir, &jb_filename).unwrap();
                        Some((jb_filename.to_owned(), jb.to_owned()))
                    } else {
                        None
                    }
                })
                .map(|(jb_filename, jb)| {
                    self.env_
                        .insert(jb_filename.to_str().unwrap().to_string(), jb.to_owned());
                    Some(())
                })
                .for_each(drop);
        } else {
            // check for the single .JB file.
            if self.path_.with_extension("JB").exists() {
                let jb_filename = filename.with_extension("JB");
                let jb = utils::read_file(dir, &jb_filename)?;

                self.env_
                    .insert(jb_filename.to_str().unwrap().to_string(), jb.to_owned());
            }
        }
        Ok(())
    }

    // Return a Vec of tuples with the filename and file contents for
    // each file that needs to be written as a backup
    fn files(&self) -> Vec<(String, String)> {
        let mut fs : Vec<(String, String)> = Vec::new();
        if let Some(jn) = &self.jobname_ {
            if let Some(script) = &self.script_ {
                fs.push((jn.to_string(), script.to_string()));
            }
        }
        for (jb, jb_contents) in self.env_.iter() {
            fs.push((jb.to_string(), jb_contents.to_string()));
        }
        fs
    }

    // Return the actual job script as a String
    fn script(&self) -> String {
        match &self.script_ {
            Some(s) => s.clone(),
            None => panic!("No script available for job {}", self.jobid_),
        }
    }

    // Return additional information as a set of key-value pairs
    fn extra_info(&self) -> Option<HashMap<String, String>> {
        Some(self.env_.clone())
    }
}

pub struct Torque {
    pub base: PathBuf,
}

impl Torque {
    pub fn new(base: &PathBuf) -> Torque {
        Torque { base: base.clone() }
    }
}

impl Scheduler for Torque {
    fn watch_locations(&self, matches: &ArgMatches) -> Vec<PathBuf> {
        if matches.is_present("subdirs") {
            (0..=9)
                .map(|sd| self.base.join(format!("{}", sd)).to_owned())
                .collect()
        } else {
            [self.base.clone()].to_vec()
        }
    }

    fn create_job_info(&self, event_path: &Path) -> Option<Box<dyn JobInfo>> {
        /*is_job_path(&event_path).map(|(jobid, dirname)| {
            let t = TorqueJobEntry::new(&dirname.to_path_buf(), jobid);
            Box::new(t)
        })*/
        if let Some((jobid, filename)) = is_job_path(&event_path) {
            Some(Box::new(TorqueJobEntry::new(
                &filename.to_path_buf(),
                jobid,
            )))
        } else {
            None
        }
    }

    fn verify_event_kind(&self, event: &Event) -> Option<Vec<PathBuf>> {
        if let Event {
            kind: EventKind::Create(CreateKind::File),
            paths,
            ..
        } = event {
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
