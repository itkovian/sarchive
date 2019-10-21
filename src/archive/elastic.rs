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

use super::Archive;
use crate::slurm::SlurmJobEntry;
use chrono::{DateTime, Utc};
use clap::{App, Arg, ArgMatches, SubCommand};
use log::{debug,error,info,warn};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::io::Error;
use std::process::exit;

/// Command line options for the elastic archiver subcommand
pub fn clap_subcommand(command: &str) -> App {
    SubCommand::with_name(command)
        .about("Archive to ElasticSearch")
        /*.arg(
            Arg::with_name("auth")
        )*/
        .arg(
            Arg::with_name("host")
                .long("host")
                .takes_value(true)
                .default_value("localhost")
                .help("The hostname of the ElasticSearch server"),
        )
        .arg(
            Arg::with_name("port")
                .long("port")
                .takes_value(true)
                .default_value("9200")
                .help("The port of the ElasticSearch service"),
        )
        .arg(
            Arg::with_name("index")
                .long("index")
                .takes_value(true)
                .required(true)
                .help("The index where the documents will be put"),
        )
}
//use elastic::http::header::{self, AUTHORIZATION, HeaderValue};
use elastic::client::{SyncClient, SyncClientBuilder};

pub struct ElasticArchive {
    client: SyncClient,
    //index: String,
}

fn create_index(client: &SyncClient, index_name: String) -> Result<(), Error> {
    let body = json!({
        "mappings": {
            "dynamic": false,
            "properties": {
                "environment": {
                    "type": "object",
                    "dynamic": true,
                },
                "id": {
                    "type": "text",
                    "fields": {
                        "keyword": {
                            "type": "keyword",
                            "ignore_above": 256
                        }
                    }
                },
                "timestamp": {
                    "type": "date"
                },
                "script": {
                    "type": "text",
                    "fields": {
                        "keyword": {
                            "type": "keyword",
                            "ignore_above": 256
                        }
                    }
                }
            }
        }
    });

    client
        .index(index_name)
        .create()
        .body(body.to_string())
        .send()
        .unwrap();

    Ok(())
}

impl SlurmJobEntry {
    pub fn read_script(&self) -> String {
        fs::read_to_string(self.path.join("script"))
            .unwrap_or_else(|_| panic!("Could not read file {:?}/script", self.path))
    }

    pub fn read_env(&self) -> HashMap<String, String> {
        let s = fs::read_to_string(self.path.join("environment"))
            .unwrap_or_else(|_| panic!("Could not read file {:?}/environment", self.path));

        s.split('\0')
            .filter(|s| !s.is_empty())
            .map(|s| {
                let ps: Vec<_> = s.split('=').collect();
                if ps.len() == 2 {
                    (ps[0].to_owned(), ps[1].to_owned())
                } else {
                    (s.to_owned(), String::from(""))
                }
            })
            .collect()
    }
}

impl ElasticArchive {
    pub fn new(host: &str, port: u16, index: String) -> Self {
        let client = SyncClientBuilder::new()
            .sniff_nodes(format!("http://{host}:{port}", host = host, port = port)) // TODO: use a pool for serde
            .build()
            .unwrap();

        // We create the index if it does not exist
        if let Ok(response) = client.index(index.to_owned()).exists().send() {
            if !response.exists() {
                create_index(&client, index.to_owned()).unwrap();
            }
        } else {
            error!("Cannot check if index exists. Quitting.");
            exit(1);
        }

        // Put the mapping once at the start of the application
        //if let Err(e) = client.document::<JobInfo>().put_mapping().send() {
        //    error!("Cannot put mapping for jobinfo document");
        //    exit(1);
        //}

        ElasticArchive {
            client,
            //index: index.to_owned(),
        }
    }

    pub fn build(matches: &ArgMatches) -> Result<Self, Error> {
        info!("Using ElasticSearch archival");
        Ok(ElasticArchive::new(
            matches.value_of("host").unwrap(),
            matches.value_of("port").unwrap().parse::<u16>().unwrap(),
            matches.value_of("index").unwrap().to_owned(),
        ))
    }
}

impl Archive for ElasticArchive {
    fn archive(&self, slurm_job_entry: &SlurmJobEntry) -> Result<(), Error> {
        debug!(
            "ES archiver, received an entry for job ID {:?}",
            slurm_job_entry.jobid
        );

        let script = slurm_job_entry.read_script();
        let env = slurm_job_entry.read_env();

        let doc = JobInfo {
            id: slurm_job_entry.jobid.to_owned(),
            timestamp: Utc::now(),
            script,
            environment: env,
        };
        let _res = self.client.document().index(doc).send().unwrap();

        Ok(())
    }
}

#[cfg(feature = "elasticsearch-7")]
#[derive(Serialize, Deserialize, ElasticType)]
struct JobInfo {
    #[elastic(id)]
    pub id: String,
    pub timestamp: DateTime<Utc>,
    pub script: String,
    pub environment: HashMap<String, String>,
}

#[cfg(test)]
mod tests {

    use super::*;
    use std::env;
    use std::path::PathBuf;

    #[test]
    fn test_read_env() {
        let path = PathBuf::from("tests/job.123456");
        let slurm_job_entry = SlurmJobEntry::new(&path, "123456");
        let hm = slurm_job_entry.read_env();

        assert_eq!(hm.len(), 46);
        assert_eq!(hm.get("SLURM_CLUSTERS").unwrap(), "cluster");
        assert_eq!(hm.get("SLURM_NTASKS_PER_NODE").unwrap(), "1");
    }
}
