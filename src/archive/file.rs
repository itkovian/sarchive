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
use clap::{App, Arg, ArgMatches};
use log::{debug, error, warn};
use std::fs::{create_dir_all, File};
use std::io::{Error, Write};
use std::path::{Path, PathBuf};

use super::Archive;
use crate::scheduler::job::JobInfo;

/// Command line options for the file archiver subcommand
pub fn clap_subcommand(command: &str) -> App {
    App::new(command)
        .about("Archive to the filesystem")
        .arg(
            Arg::new("archive")
                .long("archive")
                .short('a')
                .takes_value(true)
                .help("Location of the job scripts' archive."),
        )
        .arg(
            Arg::new("period")
                .long("period")
                .short('p')
                .takes_value(true)
                .possible_value("yearly")
                .possible_value("monthly")
                .possible_value("daily")
                .help(
                    "Archive under a YYYY subdirectory (yearly), YYYYMM (monthly), or YYYYMMDD (daily)."
                )
        )
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

/// An aerchiver that writes job script info to a file
pub struct FileArchive {
    archive_path: PathBuf,
    period: Period,
}

impl FileArchive {
    pub fn new(archive_path: &Path, p: Period) -> Self {
        FileArchive {
            archive_path: archive_path.to_path_buf(),
            period: p,
        }
    }

    pub fn build(matches: &ArgMatches) -> Result<Self, Error> {
        let archive = Path::new(
            matches
                .value_of("archive")
                .expect("You must provide the location of the archive"),
        );

        if !archive.is_dir() {
            warn!(
                "Provided archive {:?} is not a valid directory, creating it.",
                &archive
            );
            if let Err(e) = create_dir_all(archive) {
                error!("Unable to create archive at {:?}. {}", &archive, e);
                return Err(e);
            }
        };

        let period = match matches.value_of("period") {
            Some("yearly") => Period::Yearly,
            Some("monthly") => Period::Monthly,
            Some("daily") => Period::Daily,
            _ => Period::None,
        };

        Ok(FileArchive::new(archive, period))
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

    use std::fs::{create_dir, read_to_string, File};
    use std::io::Write;
    use std::path::Path;
    use tempfile::tempdir;

    use super::super::*;
    use super::*;
    use crate::scheduler::job::JobInfo;
    use crate::scheduler::slurm::SlurmJobEntry;

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

        let mut slurm_job_entry = SlurmJobEntry::new(&job_dir, "1234", "mycluster");
        if let Err(_) = slurm_job_entry.read_job_info() {
            assert!(false);
        }

        let file_archiver = FileArchive::new(&archive_dir, Period::None);
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
