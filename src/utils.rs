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
use log::{debug, warn};
use std::fs;
use std::io::{Error, ErrorKind};
use std::path::Path;
use std::thread::sleep;
use std::time::Duration;

/// Read file contents of the file given by the path. Separating the
/// directory from the filename (which may contain directory hierarchy)
/// is that we are able to monitor the path in case it dissapears (e.g.,
/// when a job is removed before we can get the information)
pub fn read_file(path: &Path, filename: &Path) -> Result<String, Error> {
    let fpath = path.join(filename);
    let mut iters = 100;
    let ten_millis = Duration::from_millis(10);
    while !Path::exists(&fpath) && iters > 0 {
        debug!("Waiting for {:?}", &fpath);
        sleep(ten_millis);
        if !Path::exists(&path) {
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
        _ => {
            let data = fs::read_to_string(&fpath)?;
            Ok(data)
        }
    }
}
