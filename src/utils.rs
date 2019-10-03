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
extern crate chrono;
extern crate crossbeam_channel;
extern crate crossbeam_utils;

use crossbeam_channel::{select, unbounded, Receiver, Sender};
use crossbeam_utils::sync::Parker;
use crossbeam_utils::Backoff;
use log::*;
use notify::event::{CreateKind, Event, EventKind};
use notify::{RecommendedWatcher, RecursiveMode, Watcher};
use std::io::Error;
use std::path::Path;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::Arc;

use super::slurm;
use crate::archive;

/// The check_and_queue function verifies that the inotify event pertains
/// and actual Slurm job entry and pushes the correct information to the
/// channel so it can be processed later on.
fn check_and_queue(s: &Sender<slurm::SlurmJobEntry>, event: Event) -> Result<(), Error> {
    debug!("Event received: {:?}", event);
    if let Event {
        kind: EventKind::Create(CreateKind::Folder),
        paths,
        ..
    } = event
    {
        if let Some((jobid, _dirname)) = slurm::is_job_path(&paths[0]) {
            let e = slurm::SlurmJobEntry::new(&paths[0], jobid);
            s.send(e).unwrap();
        };
    }
    Ok(())
}

/// The monitor function uses a platform-specific watcher to track inotify events on
/// the given path, formed by joining the base and the hash path.
/// At the same time, it check for a notification indicating that it should stop operations
/// upon receipt of which it immediately returns.
pub fn monitor(
    base: &Path,
    hash: u8,
    s: &Sender<slurm::SlurmJobEntry>,
    sigchannel: &Receiver<bool>,
) -> notify::Result<()> {
    let (tx, rx) = unbounded();

    // create a platform-specific watcher
    let mut watcher: RecommendedWatcher = Watcher::new_immediate(tx)?;
    let path = base.join(format!("hash.{}", hash));

    info!("Watching path {:?}", &path);

    if let Err(e) = watcher.watch(&path, RecursiveMode::NonRecursive) {
        return Err(e);
    }
    #[allow(clippy::zero_ptr, clippy::drop_copy)]
    loop {
        select! {
            recv(sigchannel) -> b => if let Ok(true) = b  {
                return Ok(());
            },
            recv(rx) -> event => {
                if let Ok(Ok(e)) = event { check_and_queue(s, e)? }
                else {
                    error!("Error on received event: {:?}", event);
                    break;
                }
            }
        }
    }

    Ok(())
}

/// The process function consumes job entries and call the archive function for each
/// received entry.
/// At the same time, it also checks if there is an incoming notification that it should
/// stop processing. Upon receipt, it will cease operations immediately.
pub fn process(
    archiver: &dyn archive::Archive,
    r: &Receiver<slurm::SlurmJobEntry>,
    sigchannel: &Receiver<bool>,
    cleanup: bool,
) {
    info!("Start processing events");
    #[allow(clippy::zero_ptr, clippy::drop_copy)]
    loop {
        select! {
            recv(sigchannel) -> b => if let Ok(true) = b  {
                if !cleanup {
                    info!("Stopped processing entries, {} skipped", r.len());
                } else {
                info!("Processing {} entries, then stopping", r.len());
                for entry in r.iter() {
                    archiver.archive(&entry).unwrap();
                }
                info!("Done processing");
                }
                return;
            },
            recv(r) -> entry => {
                if let Ok(slurm_job_entry) = entry {
                    archiver.archive(&slurm_job_entry).unwrap();
                } else {
                    error!("Error on receiving SlurmJobEntry info");
                    break;
                }
            }
        }
    }
}

/// This function will park the thread until it is unparked and check the
/// atomic bool to see if it should start notifying other threads they need
/// to finish execution.
pub fn signal_handler_atomic(sender: &Sender<bool>, sig: Arc<AtomicBool>, p: &Parker) {
    let backoff = Backoff::new();
    while !sig.load(SeqCst) {
        if backoff.is_completed() {
            p.park();
        } else {
            backoff.snooze();
        }
    }
    for _ in 0..20 {
        sender.send(true).unwrap();
    }
    info!("Sent 20 notifications");
}
