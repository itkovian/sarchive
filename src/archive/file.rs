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
use clap::{App, Arg, ArgMatches, SubCommand};
use std::fs::{copy, create_dir_all};
use std::io::Error;
use std::path::{Path, PathBuf};
use std::thread::sleep;
use std::time::Duration;

use super::super::slurm::SlurmJobEntry;
use super::Archive;

/// Command line options for the file archiver subcommand
pub fn clap_subcommand(command: &str) -> App {
    SubCommand::with_name(command)
        .about("Archive to the filesystem")
        .arg(
            Arg::with_name("archive")
                .long("archive")
                .short("a")
                .takes_value(true)
                .help("Location of the job scripts' archive."),
        )
        .arg(
            Arg::with_name("logfile")
                .long("logfile")
                .short("l")
                .takes_value(true)
                .help("Log file name.")
        )
        .arg(
            Arg::with_name("period")
                .long("period")
                .short("p")
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
    pub fn new(archive_path: &PathBuf, p: Period) -> Self {
        FileArchive {
            archive_path: archive_path.clone(),
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
            if let Err(e) = create_dir_all(&archive) {
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

        Ok(FileArchive::new(&archive.to_path_buf(), period))
    }
}

impl Archive for FileArchive {
    /// Archives the files from the given SlurmJobEntry's path.
    ///
    /// We busy wait for 1 second, sleeping for 10 ms per turn for
    /// the environment and script files to appear.
    /// If the files cannot be found after that tine, we output a warning
    /// and return without copying.
    /// If the directory dissapears before we found or copied the files,
    /// we panic.
    fn archive(&self, slurm_job_entry: &SlurmJobEntry) -> Result<(), Error> {
        // Simulate the debounced event we had before. Wait two seconds after dir creation event to
        // have some assurance the files will have been written.
        if slurm_job_entry.moment.elapsed().as_secs() < 2 {
            sleep(Duration::from_millis(2000) - slurm_job_entry.moment.elapsed());
        }
        let ten_millis = Duration::from_millis(10);
        // We wait for each file to be present

        let archive_path = &self.archive_path;
        let p = &self.period;
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

            let target_path = determine_target_path(&archive_path, &p, &slurm_job_entry, &filename);

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
fn determine_target_path(
    archive_path: &Path,
    p: &Period,
    slurm_job_entry: &SlurmJobEntry,
    filename: &str,
) -> PathBuf {
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
                .clone()
                .join(format!("job.{}_{}", &slurm_job_entry.jobid, &filename))
        }
        None => archive_path.join(format!("job.{}_{}", &slurm_job_entry.jobid, &filename)),
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
        env.write(b"environment");

        let job_path = job_dir.join("script");
        let mut job = File::create(&job_path).unwrap();
        job.write(b"job script");

        let slurm_job_entry = SlurmJobEntry::new(&job_dir, "1234");

        let file_archiver = FileArchive::new(&archive_dir, Period::None);
        file_archiver.archive(&slurm_job_entry);

        assert!(Path::is_file(&archive_dir.join("job.1234_environment")));
        assert!(Path::is_file(&archive_dir.join("job.1234_script")));

        let archive_env_contents =
            read_to_string(&archive_dir.join("job.1234_environment")).unwrap();
        assert_eq!(&archive_env_contents, "environment");

        let archive_script_contents = read_to_string(&archive_dir.join("job.1234_script")).unwrap();
        assert_eq!(&archive_script_contents, "job script");
    }
}
