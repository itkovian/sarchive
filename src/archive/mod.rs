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

#[cfg(feature = "kafka")]
pub mod kafka;

use clap::Subcommand;
use crossbeam_channel::{select, Receiver};
use log::{debug, error, info};
use std::io::Error;

#[cfg(feature = "kafka")]
use self::kafka::{KafkaArchive, KafkaArgs};

use super::scheduler::job::JobInfo;
use file::{FileArchive, FileArgs};
use std::thread::sleep;
use std::time::Duration;

#[derive(Subcommand)]
pub enum Archiver {
    File(FileArgs),

    #[cfg(feature = "kafka")]
    Kafka(KafkaArgs),
}

/// The Archive trait should be implemented by every backend.
#[allow(clippy::borrowed_box)]
pub trait Archive: Send {
    fn archive_creation(&self, job_entry: &Box<dyn JobInfo>) -> Result<(), Error>;
    fn archive_removal(&self, job_entry: &Box<dyn JobInfo>) -> Result<(), Error>;
}

pub fn archive_builder(archiver: &Archiver) -> Result<Box<dyn Archive>, Error> {
    match archiver {
        Archiver::File(args) => {
            let archive = FileArchive::build(args)?;
            Ok(Box::new(archive))
        }
        #[cfg(feature = "kafka")]
        Archiver::Kafka(kafka_args) => {
            let archive = KafkaArchive::build(kafka_args)?;
            Ok(Box::new(archive))
        }
    }
}

/// The process function consumes creation job entries and calls the archive function for each
/// received entry.
/// At the same time, it also checks if there is an incoming notification that it should
/// stop processing. Upon receipt, it will cease operations immediately.
pub fn process_create(
    archiver: Box<dyn Archive>,
    r: &Receiver<Box<dyn JobInfo>>,
    sigchannel: &Receiver<bool>,
    cleanup: bool,
) -> Result<(), Error> {
    info!("Start processing create events");

    #[allow(clippy::zero_ptr, clippy::drop_copy)]
    loop {
        select! {
            recv(sigchannel) -> b => if let Ok(true) = b {
                if !cleanup {
                    info!("Stopped processing entries, {} skipped", r.len());
                } else {
                    info!("Processing {} entries, then stopping", r.len());
                    for mut entry in r.iter() {
                        entry.read_job_info()?;
                        archiver.archive_creation(&entry)?;
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
                    if let Some(dur) = Duration::from_millis(2000).checked_sub(elapsed) {
                        debug!("Waiting for {} ms to elapse before checking files", dur.as_millis());
                        sleep(dur);
                    }
                    job_entry.read_job_info()?;
                    archiver.archive_creation(&job_entry)?;
                } else {
                    error!("Error on receiving JobEntry info");
                    break;
                }
            }
        }
    }

    debug!("Processing creation loop exited");
    Ok(())
}

/// The process_remove function consumes job removal events and gets the necessary data from the
/// scheduler before sending the information to the archive.
/// At the same time, it also checks if there is an incoming notification that it should
/// stop processing. Upon receipt, it will cease operations immediately.
pub fn process_remove(
    archiver: Box<dyn Archive>,
    r: &Receiver<Box<dyn JobInfo>>,
    sigchannel: &Receiver<bool>,
    cleanup: bool,
) -> Result<(), Error> {
    info!("Start processing removal events");

    #[allow(clippy::zero_ptr, clippy::drop_copy)]
    loop {
        select! {
            recv(sigchannel) -> b => if let Ok(true) = b {
                if !cleanup {
                        info!("Stopped processing entries, {} skipped", r.len());
                } else {
                    info!("Processing {} entries, then stopping", r.len());
                    for mut entry in r.iter() {
                        entry.read_job_info()?;
                        archiver.archive_removal(&entry)?;
                    }
                    info!("Done processing");
                }
                break;
            },
            recv(r) -> entry => {
                if let Ok(mut job_entry) = entry {
                    job_entry.job_completion_info()?;
                    archiver.archive_removal(&job_entry)?;
                } else {
                    error!("Error on receiving JobEntry info");
                    break;
                }
            }
        }
    }

    debug!("Processing removal loop ended");
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
        fn archive_creation(&self, _: &Box<dyn JobInfo>) -> Result<(), Error> {
            info!("Archiving creation");
            Ok(())
        }

        fn archive_removal(&self, job_entry: &Box<dyn JobInfo>) -> Result<(), Error> {
            info!("Archiving removal");
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
            s.spawn(move |_| match process_create(archiver, &rx1, &rx2, false) {
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
