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

use clap::ArgMatches;
use log::*;
use std::fs::copy;
use std::io::Error;
use std::path::{Path, PathBuf};
use std::time::Duration;
use std::thread::sleep;

use super::lib::{Period, Scheduler, SchedulerJob, determine_target_path};

pub struct Torque;

impl Scheduler for Torque {

    fn valid_path(&self, path: &Path) -> Option<Box<SchedulerJob>> {
        None
    }

    fn start_monitor(&self, base: &Path, archive: &Path, period: Period, options: Option<&ArgMatches>) {
    }
}


struct TorqueJobEntry {


}

impl SchedulerJob for TorqueJobEntry {

    fn archive(&self, archive_path: &Path, p: &Period) -> Result<(), Error> {
        Ok(())
    }

}
