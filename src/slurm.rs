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

use log::*;
use std::fs::copy;
use std::io::Error;
use std::path::{Path, PathBuf};
use std::time::Duration;
use std::thread::sleep;

use super::lib::{Period, Scheduler, SchedulerJob, determine_target_path};

pub struct Slurm;


impl Scheduler for Slurm {

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
    fn valid_path(&self, path: &Path) -> Option<Box<SchedulerJob>> {
        if path.is_dir() {
            let dirname = path.file_name().unwrap().to_str().unwrap();
            if dirname.starts_with("job.") {
                return Some(
                    Box::new(SlurmJobEntry::new(
                        &path.to_owned(), 
                        path.extension().unwrap().to_str().unwrap()
                    ))
                );
            }
        }
        debug!("{:?} is not a considered job path", &path);
        None
    }
}


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

impl SchedulerJob for SlurmJobEntry {

    /// Archives the files from the given SlurmJobEntry's path.
    /// 
    /// We busy wait for 1 second, sleeping for 10 ms per turn for
    /// the environment and script files to appear.
    /// If the files cannot be found after that tine, we output a warning
    /// and return without copying. 
    /// If the directory dissapears before we found or copied the files, 
    /// we panic.
    fn archive(&self, archive_path: &Path, p: &Period) -> Result<(), Error> {
        // We wait for each file to be present
        let ten_millis = Duration::from_millis(10);
        for filename in &["script", "environment"] {
            let fpath = self.path.join(filename);
            let mut iters = 100;
            while !Path::exists(&fpath) && iters > 0 {
                debug!("Waiting for {:?}", fpath);
                sleep(ten_millis);
                if !Path::exists(&self.path) {
                    error!("Job directory {:?} no longer exists", &self.path);
                    panic!("path not found");
                }
                iters -= 1;
            }
            if iters == 0 {
                warn!("Cannot make copy of {:?}", fpath);
                continue;
            }

            let target_path =  determine_target_path(&archive_path, &p, &self.jobid, &filename);
            
            match copy(&fpath, &target_path) {
                Ok(bytes) => info!(
                    "copied {} bytes from {:?} to {:?}",
                    bytes, &fpath, &target_path
                ),
                Err(e) => {
                    error!(
                        "Copy of {:?} to {:?} failed: {:?}",
                        &fpath, &target_path, e
                    );
                    return Err(e);
                }
            };
        }

        Ok(())
    }
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