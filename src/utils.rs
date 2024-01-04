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
use crossbeam_channel::Sender;
use crossbeam_utils::sync::{Parker, Unparker};
use crossbeam_utils::Backoff;
use log::{debug, error, info, warn};
use std::fs;
use std::io::{Error, ErrorKind};
use std::path::Path;
use std::process::exit;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::Arc;
use std::thread::sleep;
use std::time::Duration;

/// Read file contents of the file given by the path. Separating the
/// directory from the filename (which may contain directory hierarchy)
/// is that we are able to monitor the path in case it dissapears (e.g.,
/// when a job is removed before we can get the information)
///
/// We return the raw bytes, so the contents can be processed later if needed
pub fn read_file(path: &Path, filename: &Path, iters: Option<u32>) -> Result<Vec<u8>, Error> {
    let fpath = path.join(filename);
    let mut iters = iters.unwrap_or(100);
    let ten_millis = Duration::from_millis(10);
    while !Path::exists(&fpath) && iters > 0 {
        debug!("Waiting for {:?}", &fpath);
        sleep(ten_millis);
        if !Path::exists(path) {
            debug!("Job directory {:?} no longer exists", &path);
            return Err(Error::new(
                ErrorKind::NotFound,
                format!("Job directory {:?} no longer exists", &path),
            ));
        }
        iters -= 1;
    }
    match iters {
        0 => {
            warn!("Timeout waiting for {:?} to appear", &fpath);
            Err(Error::new(
                ErrorKind::NotFound,
                format!("File {:?} did not appear after waiting 1s", &fpath),
            ))
        }
        _ => fs::read(&fpath),
    }
}

/// Register the handler for the given signal, so we can properly cleanup all threads
pub fn register_signal_handler(signal: i32, unparker: &Unparker, notification: &Arc<AtomicBool>) {
    info!("Registering signal handler for signal {}", signal);
    let u1 = unparker.clone();
    let n1 = Arc::clone(notification);
    unsafe {
        if let Err(e) = signal_hook::low_level::register(signal, move || {
            info!("Received signal {}", signal);
            n1.store(true, SeqCst);
            u1.unpark()
        }) {
            error!("Cannot register signal {}: {:?}", signal, e);
            exit(1);
        }
    };
}

/// Handle the signal
pub fn signal_handler_atomic(sender: &Sender<bool>, sig: Arc<AtomicBool>, p: &Parker) {
    let backoff = Backoff::new();

    while sig.load(SeqCst) {
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

#[cfg(test)]
mod tests {

    use crossbeam_channel::unbounded;
    use crossbeam_utils::sync::Parker;
    use std::fs;
    use std::path::Path;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn test_read_file_existing_file() {
        // Setup: Create a temporary directory and file
        let temp_dir = tempdir().expect("Failed to create temporary directory");
        let file_path = temp_dir.path().join("test_file.txt");
        fs::write(&file_path, b"test contents").expect("Failed to write to test file");

        // Test: Read the contents of the existing file
        let result = read_file(temp_dir.path(), &Path::new("test_file.txt"), None);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), b"test contents");
    }

    #[test]
    fn test_read_file_nonexistent_file() {
        // Setup: Create a temporary directory without the expected file
        let temp_dir = tempdir().expect("Failed to create temporary directory");

        // Test: Attempt to read contents of a nonexistent file
        let result = read_file(temp_dir.path(), &Path::new("nonexistent_file.txt"), Some(1));
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            format!("File \"{}/nonexistent_file.txt\" did not appear after waiting 1s", temp_dir.path().display())
        );
    }

    #[test]
    fn test_register_signal_handler() {
        // Setup: Create a mock unparker and an atomic boolean
        let unparker = Parker::new();
        let notification = Arc::new(AtomicBool::new(false));

        // Test: Register a mock signal handler and trigger the signal
        register_signal_handler(1, &unparker.unparker(), &notification);
        
        // Introduce a delay to allow the signal handler to register
        std::thread::sleep(Duration::from_millis(100));

        // Trigger the signal and wait for the notification
        unsafe {
            libc::raise(1); // Simulate sending signal 1
        }

        std::thread::sleep(std::time::Duration::from_millis(100));

        unparker.unparker().unpark();
        assert!(notification.load(Ordering::SeqCst));
    }

    #[test]
    fn test_signal_handler_atomic() {
        // Setup: Create a mock sender, an atomic boolean, and a parker
        let (sender, receiver) = unbounded();
        let signal_flag = Arc::new(AtomicBool::new(false)); // Original AtomicBool
        let parker = Parker::new();

        // Test: Run the signal handler and verify notifications
        let cloned_signal_flag = Arc::clone(&signal_flag);
        std::thread::spawn(move || {
            signal_handler_atomic(&sender, cloned_signal_flag, &parker);
        });

        // Give the thread some time to start
        std::thread::sleep(Duration::from_millis(10));

        // Trigger the signal flag and wait for notifications
        {
            let flag = signal_flag;
            flag.store(true, Ordering::SeqCst);
        }

        // Introduce a delay to allow the signal handler to process and send notifications
        std::thread::sleep(Duration::from_millis(100));

        // Assert that at least one notification has been received
        assert!(receiver.try_iter().count() == 20);
    }

}
