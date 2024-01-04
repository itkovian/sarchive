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
extern crate chrono;
extern crate crossbeam_channel;
extern crate crossbeam_utils;

use crossbeam_channel::{select, unbounded, Receiver, Sender};
use log::*;
use notify::event::Event;
use notify::{recommended_watcher, RecursiveMode, Watcher};
use std::io::{Error, ErrorKind};
use std::path::Path;

use super::scheduler::job::JobInfo;
use super::scheduler::Scheduler;

/// The check_and_queue function verifies that the inotify event pertains
/// and actual Slurm job entry and pushes the correct information to the
/// channel so it can be processed later on.
#[allow(clippy::borrowed_box)]
fn check_and_queue(
    scheduler: &Box<dyn Scheduler>,
    s: &Sender<Box<dyn JobInfo>>,
    event: Event,
) -> Result<(), std::io::Error> {
    debug!("Event received: {:?}", event);

    match scheduler.verify_event_kind(&event) {
        Some(paths) => scheduler
            .create_job_info(&paths[0])
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::Other,
                    "Could not create job info structure".to_owned(),
                )
            })
            .and_then(|jobinfo| {
                s.send(jobinfo)
                    .map_err(|err| Error::new(ErrorKind::Other, err.to_string()))
            }),
        _ => Ok(()),
    }
}

/// The monitor function uses a platform-specific watcher to track inotify events on
/// the given path, formed by joining the base and the hash path.
/// At the same time, it check for a notification indicating that it should stop operations
/// upon receipt of which it immediately returns.
#[allow(clippy::borrowed_box)]
pub fn monitor(
    scheduler: &Box<dyn Scheduler>,
    path: &Path,
    s: &Sender<Box<dyn JobInfo>>,
    sigchannel: &Receiver<bool>,
) -> notify::Result<()> {
    let (tx, rx) = unbounded();

    // create a platform-specific watcher
    let mut watcher = recommended_watcher(move |res| tx.send(res).unwrap())?;

    info!("Watching path {:?}", path);

    watcher.watch(path, RecursiveMode::NonRecursive)?;

    #[allow(clippy::zero_ptr, dropping_copy_types)]
    loop {
        select! {
            recv(sigchannel) -> b => if let Ok(true) = b  {
                break Ok(());
            },
            recv(rx) -> event => {
                match event {
                    Ok(Ok(e)) => check_and_queue(scheduler, s, e)?,
                    Ok(Err(_)) | Err(_) => {
                        error!("Error on received event: {:?}", event);
                        break Err(notify::Error::new(notify::ErrorKind::Generic("Problem receiving event".to_string())));
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crossbeam_channel::unbounded;
    use notify::event::{CreateKind, Event, EventKind};
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::time::{Duration, Instant};
    use tempfile::tempdir;

    struct DummyScheduler;

    impl Scheduler for DummyScheduler {
        fn watch_locations(&self) -> Vec<PathBuf> {
            // For the purpose of the test, return a single dummy watch location
            vec!["dummy_watch_location".into()]
        }

        fn create_job_info(&self, _event_path: &Path) -> Option<Box<dyn JobInfo>> {
            // For the purpose of the test, return a dummy JobInfo instance
            Some(Box::new(DummyJobInfo))
        }

        fn verify_event_kind(&self, event: &Event) -> Option<Vec<PathBuf>> {
            if let Event {
                kind: EventKind::Create(CreateKind::File),
                ..
            } = event {
                Some(vec![event.paths[0].clone()])
            } else {
                None
            }
        }
    }

    struct DummyJobInfo;

    impl JobInfo for DummyJobInfo {
        fn jobid(&self) -> String {
            "dummy_job".to_string()
        }

        fn moment(&self) -> Instant {
            Instant::now()
        }

        fn cluster(&self) -> String {
            "dummy_cluster".to_string()
        }

        fn read_job_info(&mut self) -> Result<(), Error> {
            Ok(())
        }

        fn files(&self) -> Vec<(String, Vec<u8>)> {
            vec![]
        }

        fn script(&self) -> String {
            "dummy_script".to_string()
        }

        fn extra_info(&self) -> Option<HashMap<String, String>> {
            Some(HashMap::new())
        }
    }

    #[test]
    fn test_monitor() {
        // Setup: Create a temporary directory
        let temp_dir = tempdir().unwrap();
        let temp_dir_path = temp_dir.path().to_owned();
        let temp_dir_path_clone = temp_dir_path.clone();

        // Setup: Create a sender and receiver channels
        let (tx, rx) = unbounded();
        let (sig_tx, sig_rx) = unbounded();

        // Setup: Create a dummy scheduler
        let scheduler : Box<(dyn Scheduler + 'static)> = Box::new(DummyScheduler);

        // Test: Spawn a thread for the monitor function
        let monitor_thread = std::thread::spawn(move || {
            monitor(&scheduler, &temp_dir_path_clone, &tx, &sig_rx).expect("Monitor function failed");
        });

        // Introduce a delay to allow the monitor thread to start watching
        std::thread::sleep(Duration::from_millis(1000));

        // Test: Create a dummy file in the temporary directory
        let dummy_file_path = temp_dir_path.join("dummy_file.txt");
        std::fs::write(&dummy_file_path, "dummy_content").expect("Failed to create dummy file");

        // Introduce a delay to allow the monitor thread to detect the file event
        std::thread::sleep(Duration::from_millis(100));

        // Assert: Check if a JobInfo instance has been sent through the channel
        let job_info = rx.try_recv().expect("No JobInfo received");
        assert_eq!(job_info.jobid(), "dummy_job");

        // Signal the monitor thread to stop
        sig_tx.send(true).expect("Failed to send signal to stop the monitor thread");

        // Wait for the monitor thread to finish
        monitor_thread.join().expect("Failed to join monitor thread");
    }

    #[test]
    fn test_check_and_queue() {
        // Setup: Create a temporary directory
        let temp_dir = tempdir().unwrap();
        let temp_dir_path = temp_dir.path().to_owned();

        // Setup: Create a sender and receiver channels
        let (tx, rx) = unbounded();

        // Setup: Create a dummy scheduler
        let scheduler : Box<(dyn Scheduler + 'static)> = Box::new(DummyScheduler);

        // Test: Create a dummy file in the temporary directory
        let dummy_file_path = temp_dir_path.join("dummy_file.txt");
        std::fs::write(&dummy_file_path, "dummy_content").expect("Failed to create dummy file");

        // Test: Create a dummy event for the created file
        let dummy_event = Event {
            kind: EventKind::Create(CreateKind::File),
            paths: vec![dummy_file_path.clone()],
            ..Default::default()
        };

        // Test: Call check_and_queue function
        let result = check_and_queue(&scheduler, &tx, dummy_event);

        // Assert: Check the result and verify if JobInfo was sent through the channel
        assert!(result.is_ok());
        let job_info = rx.try_recv().expect("No JobInfo received");
        assert_eq!(job_info.jobid(), "dummy_job");
    }
}
