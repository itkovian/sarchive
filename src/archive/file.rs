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
use clap::{Args, ValueEnum};
use log::{debug, error, warn};
use std::fs::{create_dir_all, File};
use std::io::{Error, Write};
use std::path::{Path, PathBuf};

use super::Archive;
use crate::scheduler::job::JobInfo;

/// Command line options for the file archiver subcommand
#[derive(Args, Debug)]
pub struct FileArgs {
    archive: PathBuf,
    period: Period,
}

/// An enum to define a hierachy in the archive
#[derive(Clone, ValueEnum, PartialEq, Debug, Eq)]
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

/// An archiver that writes job script info to a file
pub struct FileArchive {
    archive_path: PathBuf,
    period: Period,
}

impl FileArchive {
    pub fn new(archive_path: &PathBuf, p: &Period) -> Self {
        FileArchive {
            archive_path: archive_path.to_owned(),
            period: p.to_owned(),
        }
    }

    pub fn build(args: &FileArgs) -> Result<Self, Error> {
        let archive = args.archive.to_owned();

        if !archive.is_dir() {
            warn!(
                "Provided archive {:?} is not a valid directory, creating it.",
                &archive
            );
            if let Err(e) = create_dir_all(&archive) {
                error!("Unable to create archive at {:?}. {}", &archive, e);
                return Err(e);
            }
        };

        Ok(FileArchive::new(&archive, &args.period))
    }
}

impl Archive for FileArchive {
    /// Archives the files from the given SlurmJobEntry's path.
    ///
    fn archive(&self, job_entry: &Box<dyn JobInfo>) -> Result<(), Error> {
        let archive_path = &self.archive_path;
        let target_path = determine_target_path(archive_path, &self.period);
        debug!("Target path: {:?}", target_path);
        for (fname, fcontents) in job_entry.files().iter() {
            debug!("Creating an entry for {}", fname);
            let mut f = File::create(target_path.join(fname))?;
            f.write_all(fcontents)?;
        }
        Ok(())
    }
}

/// Determines the target path for the slurm job file
///
/// The path will have the following components:
/// - the archive path
/// - a subdir depending on the Period
///     - YYYY in case of a Yearly Period
///     - YYYYMM in case of a Monthly Period
///     - YYYYMMDD in case of a Daily Period
fn determine_target_path(archive_path: &Path, p: &Period) -> PathBuf {
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
            archive_subdir_path
        }
        None => archive_path.to_path_buf(),
    }
}

#[cfg(test)]
mod tests {

    extern crate tempfile;

    use chrono::Local;
    use std::collections::HashMap;
    use std::env;
    use std::fs::{create_dir, read_to_string, remove_dir_all, File};
    use std::io::Write;
    use std::path::Path;
    use std::time::Instant;
    use tempfile::tempdir;

    use super::super::*;
    use super::*;
    use crate::scheduler::job::JobInfo;
    use crate::scheduler::slurm::SlurmJobEntry;

    #[test]
    fn test_file_archive_new() {
        let archive_path = PathBuf::from("/tmp/archive");
        let period = Period::Daily;

        let file_archive = FileArchive::new(&archive_path, &period);

        assert_eq!(file_archive.archive_path, archive_path);
        assert_eq!(file_archive.period, period);
    }

    #[test]
    fn test_file_archive_build_existing_directory() {
        let temp_dir = tempdir().unwrap();
        let archive_path = temp_dir.path().to_owned();
        let period = Period::Monthly;

        let args = FileArgs {
            archive: archive_path.clone(),
            period: period.clone(),
        };

        let file_archive = FileArchive::build(&args).unwrap();

        assert_eq!(file_archive.archive_path, archive_path);
        assert_eq!(file_archive.period, period);
    }

    #[test]
    fn test_file_archive_build_nonexistent_directory() {
        let temp_dir = tempdir().unwrap();
        let archive_path = temp_dir.path().join("nonexistent").to_owned();
        let period = Period::Yearly;

        let args = FileArgs {
            archive: archive_path.clone(),
            period: period.clone(),
        };

        let file_archive = FileArchive::build(&args).unwrap();

        assert_eq!(file_archive.archive_path, archive_path);
        assert_eq!(file_archive.period, period);
    }

    #[derive(Debug)]
    struct DummyJobInfo {
        job_id: String,
        moment: Instant,
        cluster: String,
        files: Vec<(String, Vec<u8>)>,
        script: String,
        extra_info: Option<HashMap<String, String>>,
    }

    impl DummyJobInfo {
        fn new(job_id: &str, moment: Instant, cluster: &str) -> Self {
            DummyJobInfo {
                job_id: job_id.to_string(),
                moment,
                cluster: cluster.to_string(),
                files: vec![
                    ("file1.txt".to_string(), b"contents1".to_vec()),
                    ("file2.txt".to_string(), b"contents2".to_vec()),
                ],
                script: "echo 'Hello, World!'".to_string(),
                extra_info: Some(HashMap::new()),
            }
        }
    }

    impl super::JobInfo for DummyJobInfo {
        fn jobid(&self) -> String {
            self.job_id.clone()
        }

        fn moment(&self) -> Instant {
            self.moment
        }

        fn cluster(&self) -> String {
            self.cluster.clone()
        }

        fn read_job_info(&mut self) -> Result<(), std::io::Error> {
            // Implementation for reading job info goes here...
            Ok(())
        }

        fn files(&self) -> Vec<(String, Vec<u8>)> {
            self.files.clone()
        }

        fn script(&self) -> String {
            self.script.clone()
        }

        fn extra_info(&self) -> Option<HashMap<String, String>> {
            self.extra_info.clone()
        }
    }

    #[test]
    fn test_dummy_job_info_creation() {
        let job_id = "123";
        let moment = Instant::now();
        let cluster = "test_cluster";

        let dummy_job_info = DummyJobInfo::new(job_id, moment, cluster);

        assert_eq!(dummy_job_info.jobid(), job_id);
        assert_eq!(dummy_job_info.moment(), moment);
        assert_eq!(dummy_job_info.cluster(), cluster);
        assert_eq!(
            dummy_job_info.files(),
            vec![
                ("file1.txt".to_string(), b"contents1".to_vec()),
                ("file2.txt".to_string(), b"contents2".to_vec())
            ]
        );
        assert_eq!(dummy_job_info.script(), "echo 'Hello, World!'");
        assert_eq!(dummy_job_info.extra_info(), Some(HashMap::new()));
    }

    #[test]
    fn test_dummy_job_info_read_job_info() {
        let mut dummy_job_info = DummyJobInfo::new("123", Instant::now(), "test_cluster");
        let result = dummy_job_info.read_job_info();
        assert!(result.is_ok()); // Placeholder test, assuming read_job_info always succeeds
    }

    #[test]
    fn test_dummy_job_info_files() {
        let dummy_job_info = DummyJobInfo::new("123", Instant::now(), "test_cluster");
        let files = dummy_job_info.files();
        assert_eq!(files.len(), 2);
        assert_eq!(files[0].0, "file1.txt");
        assert_eq!(files[1].0, "file2.txt");
    }

    #[test]
    fn test_file_archive_archive() {
        let temp_dir = tempdir().unwrap();
        let archive_path = temp_dir.path().to_owned();
        let period = Period::Daily;
        let job_info: Box<dyn JobInfo + 'static> =
            Box::new(DummyJobInfo::new("123", Instant::now(), "test_cluster"));

        let file_archive = FileArchive::new(&archive_path, &period);
        file_archive.archive(&job_info).unwrap();

        for (fname, fcontents) in job_info.files().iter() {
            let file_path = archive_path
                .join(format!("{}", Local::now().format("%Y%m%d")))
                .join(fname);
            assert!(file_path.exists());
            let read_contents = std::fs::read(&file_path).unwrap();
            assert_eq!(&read_contents[..], &fcontents[..]);
        }

        // Clean up
        remove_dir_all(&archive_path).unwrap();
    }

    #[test]
    fn test_determine_target_path() {
        let tdir = tempdir().unwrap();

        // create the basic archive path
        let archive_dir = tdir.path();
        let _dir = create_dir(&archive_dir);

        let p = Period::None;
        let target_path = determine_target_path(&archive_dir, &p);
        assert_eq!(target_path, archive_dir);

        let d = format!("{}", chrono::Local::now().format("%Y"));
        let p = Period::Yearly;
        let target_path = determine_target_path(&archive_dir, &p);
        assert_eq!(target_path, archive_dir.join(d));

        let d = format!("{}", chrono::Local::now().format("%Y%m"));
        let p = Period::Monthly;
        let target_path = determine_target_path(&archive_dir, &p);
        assert_eq!(target_path, archive_dir.join(d));

        let d = format!("{}", chrono::Local::now().format("%Y%m%d"));
        let p = Period::Daily;
        let target_path = determine_target_path(&archive_dir, &p);
        assert_eq!(target_path, archive_dir.join(d));
    }

    #[test]
    fn test_determine_target_path_yearly() {
        let temp_dir = env::temp_dir();
        let target_path = determine_target_path(&temp_dir, &Period::Yearly);
        assert_eq!(
            target_path,
            temp_dir.join(&format!("{}", Local::now().format("%Y")))
        );
        assert!(target_path.exists());
        remove_dir_all(&target_path).unwrap();
    }

    #[test]
    fn test_determine_target_path_monthly() {
        let temp_dir = env::temp_dir();
        let target_path = determine_target_path(&temp_dir, &Period::Monthly);
        assert_eq!(
            target_path,
            temp_dir.join(&format!("{}", Local::now().format("%Y%m")))
        );
        assert!(target_path.exists());
        remove_dir_all(&target_path).unwrap();
    }

    #[test]
    fn test_determine_target_path_daily() {
        let temp_dir = env::temp_dir();
        let target_path = determine_target_path(&temp_dir, &Period::Daily);
        assert_eq!(
            target_path,
            temp_dir.join(&format!("{}", Local::now().format("%Y%m%d")))
        );
        assert!(target_path.exists());
        remove_dir_all(&target_path).unwrap();
    }

    #[test]
    fn test_determine_target_path_none() {
        let temp_dir = env::temp_dir();
        let target_path = determine_target_path(&temp_dir, &Period::None);
        assert_eq!(target_path, temp_dir);
    }

    #[test]
    fn test_file_archive() {
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
        env.write(b"environment").unwrap();

        let job_path = job_dir.join("script");
        let mut job = File::create(&job_path).unwrap();
        job.write(b"job script").unwrap();

        let mut slurm_job_entry = SlurmJobEntry::new(&job_dir, "1234", "mycluster", &None);
        if let Err(_) = slurm_job_entry.read_job_info() {
            assert!(false);
        }

        let file_archiver = FileArchive::new(&archive_dir, &Period::None);
        let jobinfo: Box<dyn JobInfo> = Box::new(slurm_job_entry);
        file_archiver.archive(&jobinfo).unwrap();

        assert!(Path::is_file(&archive_dir.join("job.1234_environment")));
        assert!(Path::is_file(&archive_dir.join("job.1234_script")));

        let archive_env_contents =
            read_to_string(&archive_dir.join("job.1234_environment")).unwrap();
        assert_eq!(&archive_env_contents, "environment");

        let archive_script_contents = read_to_string(&archive_dir.join("job.1234_script")).unwrap();
        assert_eq!(&archive_script_contents, "job script");
    }
}
