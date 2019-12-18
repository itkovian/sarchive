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

use std::collections::HashMap;
use std::io::Error;
use std::time::Instant;

pub trait JobInfo: Send {
    // Return the job ID
    fn jobid(&self) -> String;

    // Return the moment of event occurence
    fn moment(&self) -> Instant;

    // Return the cluster to which the job was submitted
    fn cluster(&self) -> String;

    // Retrieve all the information for the job from the spool location
    // This fills up the required data structures to be able to write
    // the backup or ship the information to some consumer
    fn read_job_info(&mut self) -> Result<(), Error>;

    // Return a Vec of tuples with the filename and file contents for
    // each file that needs to be written as a backup
    fn files(&self) -> Vec<(String, String)>;

    // Return the actual job script as a String
    fn script(&self) -> String;

    // Return additional information as a set of key-value pairs
    fn extra_info(&self) -> Option<HashMap<String, String>>;
}

#[cfg(test)]
mod tests {}
