/*
Copyright 2019-2020 Andy Georges <itkovian+sarchive@gmail.com>

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

pub mod file;

#[cfg(feature = "elasticsearch-7")]
pub mod elastic;
#[cfg(feature = "kafka")]
pub mod kafka;

use clap::ArgMatches;
use crossbeam_channel::{select, Receiver};
use log::{debug, error, info, warn};
use std::io::{Error, ErrorKind};

#[cfg(feature = "elasticsearch-7")]
use self::elastic::ElasticArchive;
#[cfg(feature = "kafka")]
use self::kafka::KafkaArchive;
use super::scheduler::job::JobInfo;
use file::FileArchive;
use std::thread::sleep;
use std::time::Duration;

/// The Archive trait should be implemented by every backend.
#[allow(clippy::borrowed_box)]
pub trait Archive: Send {
    fn archive(&self, slurm_job_entry: &Box<dyn JobInfo>) -> Result<(), Error>;
}

pub fn archive_builder(matches: &ArgMatches) -> Result<Box<dyn Archive>, Error> {
    match matches.subcommand() {
        ("file", Some(command_matches)) => {
            let archive = FileArchive::build(command_matches)?;
            Ok(Box::new(archive))
        }
        #[cfg(feature = "elasticsearch-7")]
        ("elasticsearch", Some(run_matches)) => {
            let archive = ElasticArchive::build(run_matches)?;
            Ok(Box::new(archive))
        }
        #[cfg(feature = "kafka")]
        ("kafka", Some(run_matches)) => {
            let archive = KafkaArchive::build(run_matches)?;
            Ok(Box::new(archive))
        }
        (&_, _) => Err(Error::new(
            ErrorKind::Other,
            "No supported archival subcommand used",
        )),
    }
}

/// The process function consumes job entries and call the archive function for each
/// received entry.
/// At the same time, it also checks if there is an incoming notification that it should
/// stop processing. Upon receipt, it will cease operations immediately.
pub fn process(
    archiver: Box<dyn Archive>,
    r: &Receiver<Box<dyn JobInfo>>,
    sigchannel: &Receiver<bool>,
    cleanup: bool,
) -> Result<(), Error> {
    info!("Start processing events");

    #[allow(clippy::zero_ptr, clippy::drop_copy)]
    loop {
        select! {
            recv(sigchannel) -> b => if let Ok(true) = b  {
                if !cleanup {
                    info!("Stopped processing entries, {} skipped", r.len());
                } else {
                    info!("Processing {} entries, then stopping", r.len());
                    for mut entry in r.iter() {
                        entry.read_job_info()?;
                        archiver.archive(&entry)?;
                    }
                    info!("Done processing");
                }
                break;
            },
            recv(r) -> entry => {
                if let Ok(mut job_entry) = entry {
                    // Simulate the debounced event we had before. Wait two seconds after dir creation event to
                    // have some assurance the files will have been written.
                    let elapsed = job_entry.moment().elapsed();
                    if elapsed.as_millis() < 2000 {
                        debug!("Waiting for time to elapse before checking files");
                        if let Some(dur) = Duration::from_millis(2000).checked_sub(elapsed) {
                            sleep(dur);
                        } else {
                            warn!("Time elapse check failed: expect less than 2000 millis, got {}", elapsed.as_millis());
                        }
                    }
                    job_entry.read_job_info()?;
                    archiver.archive(&job_entry)?;
                } else {
                    error!("Error on receiving JobEntry info");
                    break;
                }
            }
        }
    }

    debug!("Processing loop exited");
    Ok(())
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::scheduler::job::JobInfo;
    use crate::scheduler::slurm::SlurmJobEntry;
    use crossbeam_channel::unbounded;
    use crossbeam_utils::thread::scope;
    use std::env::current_dir;
    use std::path::PathBuf;
    use std::thread::sleep;
    use std::time::Duration;

    struct DummyArchiver;

    impl Archive for DummyArchiver {
        fn archive(&self, _: &Box<dyn JobInfo>) -> Result<(), Error> {
            info!("Archiving");
            Ok(())
        }
    }

    #[test]
    fn test_process() {
        let (tx1, rx1) = unbounded();
        let (tx2, rx2) = unbounded();
        let archiver = Box::new(DummyArchiver);

        scope(|s| {
            let path = PathBuf::from(current_dir().unwrap().join("tests/job.123456"));
            let slurm_job_entry = SlurmJobEntry::new(&path, "123456", "mycluster");
            s.spawn(move |_| match process(archiver, &rx1, &rx2, false) {
                Ok(v) => assert_eq!(v, ()),
                Err(_) => panic!("Unexpected error from process function"),
            });
            tx1.send(Box::new(slurm_job_entry)).unwrap();
            sleep(Duration::from_millis(1000));
            tx2.send(true).unwrap();
        })
        .unwrap();
    }
}
