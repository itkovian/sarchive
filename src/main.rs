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
use crossbeam_utils::sync::Parker;
use crossbeam_utils::thread::scope;
use log::{error, info};
use std::path::{Path, PathBuf};
use std::process::exit;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

mod archive;
mod monitor;
mod scheduler;
mod utils;

#[cfg(feature = "elasticsearch-7")]
use archive::elastic as el;
use archive::file;
#[cfg(feature = "kafka")]
use archive::kafka as kf;
use archive::{archive_builder, process, Archive};
use monitor::monitor;
use scheduler::{create, SchedulerKind};
use utils::{register_signal_handler, signal_handler_atomic};

const VERSION: &str = env!("CARGO_PKG_VERSION");

fn setup_logging(debug: bool, logfile: Option<&str>) -> Result<(), log::SetLoggerError> {
    let level_filter = if debug {
        log::LevelFilter::Debug
    } else {
        log::LevelFilter::Info
    };

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
                .required(true)
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
            Arg::with_name("logfile")
                .long("logfile")
                .short("l")
                .takes_value(true)
                .help("Log file name.")
        )
        .arg(
            Arg::with_name("scheduler")
                .long("scheduler")
                .takes_value(true)
                .default_value("slurm")
                .possible_values(&["slurm", "torque"])
                .help("Supported schedulers")
        )
        .arg(Arg::with_name("torque-subdirs ")
            .long("torque-subdirs")
            .help("Monitor the subdirs 0...9 in the torque spool directory")
        )
        .arg(
            Arg::with_name("spool")
                .long("spool")
                .short("s")
                .takes_value(true)
                .help(
                    "Location of the Torque job spool (where the job scripts and XML files are kept).",
                )
        )
        .subcommand(file::clap_subcommand("file"));

    #[cfg(feature = "elasticsearch-7")]
    let matches = matches.subcommand(el::clap_subcommand("elasticsearch"));

    #[cfg(feature = "kafka")]
    let matches = matches.subcommand(kf::clap_subcommand("kafka"));

    matches.get_matches()
}

fn main() -> Result<(), std::io::Error> {
    let matches = args();

    match setup_logging(matches.is_present("debug"), matches.value_of("logfile")) {
        Ok(_) => (),
        Err(e) => panic!("Cannot set up logging: {:?}", e),
    };
    let base = Path::new(
        matches
            .value_of("spool")
            .expect("You must provide the location of the spool dir."),
    );
    // FIXME: Check for permissions to read directory contents
    if !base.is_dir() {
        error!("Provided spool {:?} is not a valid directory", base);
        exit(1);
    }

    let scheduler_kind = match matches.value_of("scheduler") {
        Some("slurm") => SchedulerKind::Slurm,
        Some("torque") => SchedulerKind::Torque,
        _ => panic!("Unsupported scheduler"), // This should have been handled by clap, so never arrive here
    };
    let archiver: Box<dyn Archive> = archive_builder(&matches).unwrap();
    let cluster = matches
        .value_of("cluster")
        .expect("Cluster argument is mandatory");

    info!("sarchive starting. Watching spool {:?}.", &base);

    let notification = Arc::new(AtomicBool::new(false));
    let parker = Parker::new();
    let unparker = parker.unparker();

    register_signal_handler(signal_hook::SIGTERM, &unparker, &notification);
    register_signal_handler(signal_hook::SIGINT, &unparker, &notification);

    let (sig_sender, sig_receiver) = bounded(20);
    let cleanup = matches.is_present("cleanup");

    // we will watch the locations provided by the scheduler
    let (sender, receiver) = unbounded();
    let sched = create(&scheduler_kind, &base.to_path_buf(), cluster);
    if let Err(e) = scope(|s| {
        let ss = &sig_sender;
        s.spawn(move |_| {
            signal_handler_atomic(ss, notification, &parker);
            info!("Signal handled");
        });

        for loc in sched.watch_locations(&matches) {
            let t = &sender;
            let sr = &sig_receiver;
            let sl = &sched;
            s.spawn(move |_| match monitor(sl, &loc, t, sr) {
                Ok(_) => info!("Stopped watching location {:?}", &loc),
                Err(e) => {
                    error!("{:?}", e);
                    panic!("Error watching {:?}", &base);
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
