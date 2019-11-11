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

#[cfg(feature = "elasticsearch-7")]
pub mod elastic;
pub mod file;
#[cfg(feature = "kafka")]
pub mod kafka;

#[cfg(feature = "elasticsearch-7")]
use self::elastic::ElasticArchive;
use super::slurm;
use clap::ArgMatches;
use file::FileArchive;
#[cfg(feature = "kafka")]
use self::kafka::KafkaArchive;
use std::io::{Error, ErrorKind};

/// The Archive trait should be implemented by every backend.
pub trait Archive: Send {
    fn archive(&self, slurm_job_entry: &slurm::SlurmJobEntry) -> Result<(), Error>;
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
