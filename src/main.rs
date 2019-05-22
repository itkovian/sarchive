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
extern crate clap;
extern crate crossbeam_channel;
extern crate crossbeam_utils;
extern crate notify;

#[macro_use]
extern crate slog;
extern crate slog_term;

use clap::{App, Arg};
use crossbeam_channel::unbounded;
use crossbeam_utils::thread::scope;
use slog::*;
use std::path::Path;
use std::process::exit;

mod lib;
use lib::{monitor, process};

fn main() {

    let plain = slog_term::PlainSyncDecorator::new(std::io::stdout());
    let logger = Logger::root(
        slog_term::FullFormat::new(plain)
        .build().fuse(), o!()
    );

    let matches = App::new("SArchive")
        .version("0.1.0")
        .author("Andy Georges <itkovian+sarchive@gmail.com>")
        .about("Archive slurm user job scripts.")
        .arg(
            Arg::with_name("archive")
                .long("archive")
                .short("a")
                .takes_value(true)
                .help("Location of the job scripts' archive."),
        )
        .arg(
            Arg::with_name("cluster")
                .long("cluster")
                .short("c")
                .takes_value(true)
                .help("Name of the cluster where the jobs have been submitted to."),
        )
        .arg(
            Arg::with_name("spool")
                .long("spool")
                .short("s")
                .takes_value(true)
                .help(
                    "Location of the Slurm StateSaveLocation (where the job hash dirs are kept).",
                ),
        )
        .get_matches();

    let base = Path::new(
        matches
            .value_of("spool")
            .expect("You must provide the location of the hash dirs."),
    );
    let archive = Path::new(
        matches
            .value_of("archive")
            .expect("You must provide the location of the archive"),
    );

    if !base.is_dir() {
        error!(logger, "Provided base {:?} is not a valid directory", base);
        exit(1);
    }
    if !archive.is_dir() {
        error!(logger, "Provided archive {:?} is not a valid directory", archive);
        exit(1);
    }

    // we will watch the ten hash.X directories
    let hash_range = 0..10;
    let (sender, receiver) = unbounded();
    scope(|s| {
        for hash in hash_range {
            info!(logger, "Watching hash.{}", &hash);
            let h = hash.clone(); 
            let t = &sender;
            let l = &logger;
            s.spawn(move |_| {
                match monitor(base, h, t) {
                    Ok(_) => warn!(l, "Stopped watching hash.{}", h),
                    Err(e) => { 
                        error!(l, "{}", format!("{:?}", e));
                        panic!("Error watching hash.{}", h);
                    }
                }});
        }
        let r = &receiver;
        s.spawn(move |_| process(archive, r));
    });

    info!(logger, "Sarchive finished");
    exit(0);
}
