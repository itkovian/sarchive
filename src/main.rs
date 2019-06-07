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
use crossbeam_channel::{bounded, unbounded};
use crossbeam_utils::thread::scope;
use std::fs::create_dir_all;
use std::path::Path;
use std::process::exit;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread::sleep;
use std::time::Duration;

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

    info!("sarchive starting. Watching hash dirs in {:?}. Archiving under {:?}.", &base, &archive);

    if !base.is_dir() {
        error!("Provided base {:?} is not a valid directory", base);
        exit(1);
    }
    if !archive.is_dir() {
        warn!("Provided archive {:?} is not a valid directory, creating it.", &archive);
        if let Err(e) = create_dir_all(&archive) {
            error!("Unable to create archive at {:?}. {}", &archive, e);
            exit(1);
        }
    }

    let sig = Arc::new(AtomicUsize::new(0));
    const SIGTERM: usize = signal_hook::SIGTERM as usize;
    const SIGINT: usize = signal_hook::SIGINT as usize;

    info!("Registering signal handler for SIGTERM");
    signal_hook::flag::register_usize(signal_hook::SIGTERM, Arc::clone(&sig),  SIGTERM);

    info!("Registering signal handler for SIGINT");
    signal_hook::flag::register_usize(signal_hook::SIGINT, Arc::clone(&sig), SIGINT);

    let (sig_sender, sig_receiver) = bounded(100);
    // we will watch the ten hash.X directories
    let (sender, receiver) = unbounded();
    if let Err(e) = scope(|s| {

        let ss = &sig_sender;
        s.spawn(move |_| {
            let ten_millis = Duration::from_millis(10);
            loop {
                match sig.load(Ordering::Relaxed) {
                    0 => {
                        sleep(ten_millis);
                    },
                    SIGTERM | SIGINT => {
                        for i in 0..20 {
                            sig_sender.send(true);
                        }
                        break;
                    }
                    _ => unreachable!()
                }
            }
        });
        for hash in 0..10 {
            let t = &sender;
            let h = hash;
            let sr = &sig_receiver;
            s.spawn(move |_| match monitor(base, hash, t, sr) {
                Ok(_) => info!("Stopped watching hash.{}", &h),
                Err(e) => {
                    error!("{}", e);
                    panic!("Error watching hash.{}", &h);
                }
            });
        }
        let r = &receiver;
        let sr = &sig_receiver;
        s.spawn(move |_| process(archive, period, r, sr));
    }) {
        error!("sarchive stopping due to error: {:?}", e);
        exit(1);
    };

    info!("Sarchive finished");
    exit(0);
}
