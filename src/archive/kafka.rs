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
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::get_rdkafka_version;
use log::{debug, error, info};
use std::collections::HashMap;
use std::fs;
use std::io::Error;
use std::process::exit;


pub fn clap_subcommand(command: &str) -> App {
    SubCommand::with_name(command)
        .about("Archive to Kafka")
        .arg(Arg::with_name("brokers")
            .long("brokers")
            .takes_value(true)
            .default_value("localhost:9092")
            .help("Comma-separated list of brokers")
        )
        .arg(Arg::with_name("topic")
            .long("topic")
            .takes_value(true)
            .default_value("sarchive")
            .help("Topic under which to send messages to Kafka")
        )
        .arg(Arg::with_name("message_timeout")
            .long("message.timeout")
            .takes_value(true)
            .default_value("5000")
            .help("Message timout in ms")
        )
}


pub struct KafkaArchive {
    producer: FutureProducer,
    topic: String
}

impl KafkaArchive {

    pub fn new(brokers: &str, topic: &str, message_timeout: &str) -> Self {
        KafkaArchive {
            producer: ClientConfig::new()
                .set("bootstrap.servers", brokers)
                .set("message.timeout.ms", message_timeout)
                .create()
                .expect("Cannot create Kafka producer. Aborting."),
            topic: topic.to_owned()
        }
    }

    pub fn build(matches: &ArgMatches) -> Result<Self, Error> {
        info!("Using ElasticSearch archival");
        Ok(KafkaArchive::new(
            matches.value_of("brokers").unwrap(),
            matches.value_of("topic").unwrap(),
            matches.value_of("message_timeout").unwrap()
        ))
    }

}


impl Archive for KafkaArchive {
    fn archive(&self, slurm_job_entry: &SlurmJobEntry) -> Result<(), Error> {
        Ok(())
    }
}