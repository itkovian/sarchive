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
extern crate chrono;
extern crate clap;
extern crate crossbeam_channel;
extern crate crossbeam_utils;
extern crate notify;

#[macro_use]
extern crate log;
extern crate fern;
extern crate syslog;

use clap::{App, Arg};
use crossbeam_channel::unbounded;
use crossbeam_utils::thread::scope;
use std::fs::create_dir_all;
use std::io;
use std::path::Path;
use std::process::exit;

mod lib;
use lib::{monitor, process, Period};

fn setup_logging(level_filter: log::LevelFilter, logfile: Option<&str>) -> Result<(), log::SetLoggerError> {
    let base_config = fern::Dispatch::new()
        .format(|out, message, record| {
            out.finish(format_args!(
                "[{}][{}][{}] {}",
                chrono::Local::now().to_rfc3339(),
                record.target(),
                record.level(),
                message
            ))
        })
        .level(level_filter);

    match logfile {
        Some(filename) => base_config.chain(fern::log_file(filename).unwrap()),
        None => base_config.chain(std::io::stdout())
    }.apply()
}

fn main() {
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
            Arg::with_name("debug")
                .long("debug")
                .help("Log at DEBUG level.")
        )
        .arg(
            Arg::with_name("logfile")
                .long("logfile")
                .short("l")
                .takes_value(true)
                .help("Log file name.")
        )
        .arg(
            Arg::with_name("period")
                .long("period")
                .short("p")
                .takes_value(true)
                .possible_value("yearly")
                .possible_value("monthly")
                .possible_value("daily")
                .help(
                    "Archive under a YYYY subdirectory (yearly), YYYYMM (monthly), or YYYYMMDD (daily)."
                )
        )
        .arg(
            Arg::with_name("spool")
                .long("spool")
                .short("s")
                .takes_value(true)
                .help(
                    "Location of the Slurm StateSaveLocation (where the job hash dirs are kept).",
                )
        )
        .get_matches();

    let log_level = if matches.is_present("debug") {
        log::LevelFilter::Debug
    } else {
        log::LevelFilter::Info
    };
    match setup_logging(log_level, matches.value_of("logfile")) {
        Ok(_) => (),
        Err(e) => panic!("Cannot set up logging: {:?}", e)
    };

    let period = match matches.value_of("period") {
        Some("yearly") => Period::Yearly,
        Some("monthly") => Period::Monthly,
        Some("daily") => Period::Daily,
        _ => Period::None,
    };

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
        error!("Provided base {:?} is not a valid directory", base);
        exit(1);
    }
    if !archive.is_dir() {
        warn!("Provided archive {:?} is not a valid directory, creating it.", &archive);
        match create_dir_all(&archive) {
            Err(e) => {
                error!("Unable to create archive at {:?}", &archive);
                exit(1);
            },
            _ => ()
        }
    }

    // we will watch the ten hash.X directories
    let (sender, receiver) = unbounded();
    scope(|s| {
        for hash in 0..10 {
            info!("Watching hash.");
            let h = hash.clone();
            let t = &sender;
            s.spawn(move |_| match monitor(base, h, t) {
                Ok(_) => info!("Stopped watching hash"),
                Err(e) => {
                    error!("{}", e);
                    panic!("Error watching hash.{}", h);
                }
            });
        }
        let r = &receiver;
        s.spawn(move |_| process(archive, period, r));
    });

    info!("Sarchive finished");
    exit(0);
}
