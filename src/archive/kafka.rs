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
        )
        .arg(Arg::with_name("topic")
            .long("topic")
            .takes_value(true)
            .default_value("sarchive")
        )
}