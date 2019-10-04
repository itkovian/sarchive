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
extern crate elastic;
#[macro_use]
extern crate elastic_derive;
extern crate fern;
extern crate libc;
#[macro_use]
extern crate log;
extern crate notify;
extern crate reopen;
#[macro_use]
extern crate serde_json;
extern crate syslog;

use clap::{App, Arg, SubCommand};
use crossbeam_channel::{bounded, unbounded};
use crossbeam_utils::sync::Parker;
use crossbeam_utils::thread::scope;
use std::fs::create_dir_all;
use std::path::{Path, PathBuf};
use std::process::exit;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::Arc;

mod archive;
mod slurm;
mod utils;

use archive::file::{FileArchive, Period};
use utils::{monitor, process, signal_handler_atomic};

fn setup_logging(
    level_filter: log::LevelFilter,
    logfile: Option<&str>,
) -> Result<(), log::SetLoggerError> {
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
        Some(filename) => {
            let r = fern::log_reopen(&PathBuf::from(filename), Some(libc::SIGHUP)).unwrap();
            base_config.chain(r)
        }
        None => base_config.chain(std::io::stdout()),
    }
    .apply()
}

fn main() {
    let matches = App::new("SArchive")
        .version("0.6.0")
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
            Arg::with_name("cleanup")
                .long("cleanup")
                .help(
                    "[Experimental] Process already received events when the program is terminated with SIGINT or SIGTERM"
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
        .subcommand(SubCommand::with_name("file")
            .about("Archive to the filesystem")
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
        )
        .subcommand(SubCommand::with_name("elasticsearch")
            .about("Archive to ElasticSearch")
            .arg(
                Arg::with_name("host")
                    .long("host")
                    .takes_value(true)
                    .default_value("localhost")
                    .help("The hostname of the ElasticSearch server")
            )
            .arg(
                Arg::with_name("port")
                    .long("port")
                    .takes_value(true)
                    .default_value("9200")
                    .help("The port of the ElasticSearch service")
            )
            .arg(
                Arg::with_name("index")
                    .long("index")
                    .takes_value(true)
                    .required(true)
                    .help("The index where the documents will be put")
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
        Err(e) => panic!("Cannot set up logging: {:?}", e),
    };
    let base = Path::new(
        matches
            .value_of("spool")
            .expect("You must provide the location of the hash dirs."),
    );
    if !base.is_dir() {
        error!("Provided base {:?} is not a valid directory", base);
        exit(1);
    }

    let archiver: Box<dyn Archive> = match matches.subcommand() {
        ("file", Some(command_matches)) => {
            let archive = Path::new(
                command_matches
                    .value_of("archive")
                    .expect("You must provide the location of the archive"),
            );

            if !archive.is_dir() {
                warn!(
                    "Provided archive {:?} is not a valid directory, creating it.",
                    &archive
                );
                if let Err(e) = create_dir_all(&archive) {
                    error!("Unable to create archive at {:?}. {}", &archive, e);
                    exit(1);
                }
            };

            let period = match command_matches.value_of("period") {
                Some("yearly") => Period::Yearly,
                Some("monthly") => Period::Monthly,
                Some("daily") => Period::Daily,
                _ => Period::None,
            };

            Box::new(FileArchive::new(&archive.to_path_buf(), period))
        }
        ("elasticsearch", Some(run_matches)) => {
            info!("Using ElasticSearch archival");
            Box::new(ElasticArchive::new(
                run_matches.value_of("host").unwrap(),
                run_matches
                    .value_of("port")
                    .unwrap()
                    .parse::<u16>()
                    .unwrap(),
                run_matches.value_of("index").unwrap().to_owned(),
            ))
        }
        (&_, _) => {
            error!("No matching subcommand used, exiting");
            exit(1);
        }
    };

    info!("sarchive starting. Watching hash dirs in {:?}.", &base);

    let notification = Arc::new(AtomicBool::new(false));
    let parker = Parker::new();
    let unparker = parker.unparker().clone();

    info!("Registering signal handler for SIGTERM");
    let u1 = unparker.clone();
    let n1 = Arc::clone(&notification);
    unsafe {
        if let Err(e) = signal_hook::register(signal_hook::SIGTERM, move || {
            info!("Received SIGTERM");
            n1.store(true, SeqCst);
            u1.unpark()
        }) {
            error!("Cannot register signal for SIGTERM: {:?}", e);
            exit(1);
        }
    };

    info!("Registering signal handler for SIGINT");
    let u2 = unparker.clone();
    let n2 = Arc::clone(&notification);
    unsafe {
        if let Err(e) = signal_hook::register(signal_hook::SIGINT, move || {
            info!("Received SIGINT");
            n2.store(true, SeqCst);
            u2.unpark()
        }) {
            error!("Cannot register signal for SIGTERM: {:?})", e);
            exit(1);
        }
    };

    let (sig_sender, sig_receiver) = bounded(20);

    let cleanup = matches.is_present("cleanup");

    // we will watch the ten hash.X directories
    let (sender, receiver) = unbounded();
    if let Err(e) = scope(|s| {
        let ss = &sig_sender;
        s.spawn(move |_| {
            signal_handler_atomic(ss, notification, &parker);
            info!("Signal handled");
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
        s.spawn(move |_| process(&archiver, r, sr, cleanup));
    }) {
        error!("sarchive stopping due to error: {:?}", e);
        exit(1);
    };

    info!("Sarchive finished");
    exit(0);
}
