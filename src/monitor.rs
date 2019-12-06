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
use log::*;
use notify::event::{CreateKind, Event, EventKind};
use notify::{RecommendedWatcher, RecursiveMode, Watcher};
use std::io::Error;
use std::path::Path;

use super::scheduler::slurm;
use super::scheduler::Scheduler;
use super::scheduler::job::JobInfo;

/// The check_and_queue function verifies that the inotify event pertains
/// and actual Slurm job entry and pushes the correct information to the
/// channel so it can be processed later on.
fn check_and_queue(scheduler: &dyn Scheduler, s: &Sender<Box<dyn JobInfo>>, event: Event) -> Result<(), Error> {
    debug!("Event received: {:?}", event);
    if let Event {
        kind: EventKind::Create(CreateKind::Folder),
        paths,
        ..
    } = event
    {
        debug!("Received event for path {:?}", &paths[0]);
        if let Some(jobinfo) = scheduler.create_job_info(&paths[0]) {
            debug!("Sending jobinfo");
            s.send(jobinfo).unwrap();
        }
    }
    Ok(())
}

/// The monitor function uses a platform-specific watcher to track inotify events on
/// the given path, formed by joining the base and the hash path.
/// At the same time, it check for a notification indicating that it should stop operations
/// upon receipt of which it immediately returns.
pub fn monitor(
    scheduler: &dyn Scheduler,
    base: &Path,
    hash: u8,
    s: &Sender<Box<dyn JobInfo>>,
    sigchannel: &Receiver<bool>,
) -> notify::Result<()> {
    let (tx, rx) = unbounded();

    // create a platform-specific watcher
    let mut watcher = RecommendedWatcher::new_immediate(move |res| tx.send(res).unwrap())?;
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
                if let Ok(Ok(e)) = event {
                    check_and_queue(scheduler, s, e)?
                } else {
                    error!("Error on received event: {:?}", event);
                    break;
                }
            }
        }
    }

    Ok(())
}

