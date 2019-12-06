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

use chrono;
use clap::{App, Arg, ArgMatches};
use crossbeam_channel::{bounded, unbounded};
use crossbeam_utils::sync::{Parker, Unparker};
use crossbeam_utils::thread::scope;
use log::{error, info};
use std::path::{Path, PathBuf};
use std::process::exit;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::Arc;

mod archive;
mod scheduler;
mod utils;

#[cfg(feature = "elasticsearch-7")]
use archive::elastic as el;
use archive::file;
#[cfg(feature = "kafka")]
use archive::kafka as kf;
use archive::{archive_builder, Archive};
use utils::{monitor, process, signal_handler_atomic};

const VERSION: &str = env!("CARGO_PKG_VERSION");

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

fn args<'a>() -> ArgMatches<'a> {
    let matches = App::new("SArchive")
        .version(VERSION)
        .author("Andy Georges <itkovian+sarchive@gmail.com>")
        .about("Archive slurm user job scripts.")
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
        .subcommand(file::clap_subcommand("file"));

    #[cfg(feature = "elasticsearch-7")]
    let matches = matches.subcommand(el::clap_subcommand("elasticsearch"));
    #[cfg(feature = "kafka")]
    let matches = matches.subcommand(kf::clap_subcommand("kafka"));
    matches.get_matches()
}

fn register_signal_handler(signal: i32, unparker: &Unparker, notification: &Arc<AtomicBool>) {
    info!("Registering signal handler for signal {}", signal);
    let u1 = unparker.clone();
    let n1 = Arc::clone(&notification);
    unsafe {
        if let Err(e) = signal_hook::register(signal, move || {
            info!("Received signal {}", signal);
            n1.store(true, SeqCst);
            u1.unpark()
        }) {
            error!("Cannot register signal {}: {:?}", signal, e);
            exit(1);
        }
    };
}

fn main() {
    let matches = args();

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

    let archiver: Box<dyn Archive> = archive_builder(&matches).unwrap();

    info!("sarchive starting. Watching hash dirs in {:?}.", &base);

    let notification = Arc::new(AtomicBool::new(false));
    let parker = Parker::new();
    let unparker = parker.unparker();

    register_signal_handler(signal_hook::SIGTERM, &unparker, &notification);
    register_signal_handler(signal_hook::SIGINT, &unparker, &notification);

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
        s.spawn(move |_| process(archiver, r, sr, cleanup));
    }) {
        error!("sarchive stopping due to error: {:?}", e);
        exit(1);
    };

    info!("Sarchive finished");
    exit(0);
}
