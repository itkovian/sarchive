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
    fn files(&self) -> Vec<(String, Vec<u8>)>;

    // Return the actual job script as a String
    fn script(&self) -> String;

    // Return additional information as a set of key-value pairs
    fn extra_info(&self) -> Option<HashMap<String, String>>;
}

#[cfg(test)]
mod tests {

    use std::collections::HashMap;
    use std::time::{Instant, Duration};
    use std::thread::sleep;

    use super::*;

    #[derive(Debug)]
    struct DummyJobInfo {
        job_id: String,
        moment: Instant,
        cluster: String,
        script: String,
        extra_info: Option<HashMap<String, String>>,
        files: Vec<(String, Vec<u8>)>,
    }

    impl DummyJobInfo {
        fn new(job_id: &str, cluster: &str, script: &str, extra_info: Option<HashMap<String, String>>) -> Self {
            DummyJobInfo {
                job_id: job_id.to_string(),
                moment: Instant::now(),
                cluster: cluster.to_string(),
                script: script.to_string(),
                extra_info,
                files: Vec::new(),
            }
        }

        fn add_file(&mut self, filename: &str, contents: Vec<u8>) {
            self.files.push((filename.to_string(), contents));
        }
    }

    impl JobInfo for DummyJobInfo {
        fn jobid(&self) -> String {
            self.job_id.clone()
        }

        fn moment(&self) -> Instant {
            self.moment
        }

        fn cluster(&self) -> String {
            self.cluster.clone()
        }

        fn read_job_info(&mut self) -> Result<(), Error> {
            // Simulate reading job information (e.g., from spool location)
            // This is just a mock implementation, adjust it based on your actual logic.

            // Simulate some delay for reading job info
            sleep(Duration::from_millis(50));

            // Simulate filling data structures
            // (e.g., populating the 'files' vector)
            self.add_file("file1.txt", b"file1 contents".to_vec());
            self.add_file("file2.txt", b"file2 contents".to_vec());

            Ok(())
        }

        fn files(&self) -> Vec<(String, Vec<u8>)> {
            self.files.clone()
        }

        fn script(&self) -> String {
            self.script.clone()
        }

        fn extra_info(&self) -> Option<HashMap<String, String>> {
            self.extra_info.clone()
        }
    }

    #[test]
    fn test_jobid() {
        let job_info = DummyJobInfo::new("job123", "cluster1", "script1", None);
        assert_eq!(job_info.jobid(), "job123");
    }

    #[test]
    fn test_moment() {
        let job_info = DummyJobInfo::new("job123", "cluster1", "script1", None);
        assert!(job_info.moment() <= Instant::now());
    }

    #[test]
    fn test_cluster() {
        let job_info = DummyJobInfo::new("job123", "cluster1", "script1", None);
        assert_eq!(job_info.cluster(), "cluster1");
    }

    #[test]
    fn test_read_job_info() {
        let mut job_info = DummyJobInfo::new("job123", "cluster1", "script1", None);
        assert!(job_info.files().is_empty());

        job_info.read_job_info().unwrap();
        assert_eq!(job_info.files().len(), 2);
    }

    #[test]
    fn test_script() {
        let job_info = DummyJobInfo::new("job123", "cluster1", "script1", None);
        assert_eq!(job_info.script(), "script1");
    }

    #[test]
    fn test_extra_info() {
        let extra_info = {
            let mut map = HashMap::new();
            map.insert("key1".to_string(), "value1".to_string());
            map.insert("key2".to_string(), "value2".to_string());
            map
        };

        let job_info = DummyJobInfo::new("job123", "cluster1", "script1", Some(extra_info.clone()));
        assert_eq!(job_info.extra_info(), Some(extra_info));
    }
}


