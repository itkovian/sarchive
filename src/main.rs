extern crate clap;
extern crate notify;

use clap::{Arg, App};
//use crossbeam_channel::{unbounded};
use notify::{DebouncedEvent, RecommendedWatcher, RecursiveMode, Result, Watcher};
use std::path::{Path};
use std::sync::mpsc::channel;
use std::time::Duration;


fn archive_script(archive: &Path, event: DebouncedEvent) -> Result<()> {
    println!("Event received: {:?}", event);
    Ok(())
}


fn watch_and_copy(archive: &Path, base: &Path, hash: u8) -> notify::Result<()> {

    let (tx, rx) = channel();

    // create a platform-specifi watcher
    let mut watcher:RecommendedWatcher = Watcher::new(tx, Duration::from_secs(2))?;  
    let path = base.join(format!("hash.{}", hash));

    // TODO: check the path exists!

    watcher.watch(path, RecursiveMode::Recursive)?;

    loop {
        match rx.recv() {
            Ok(event) => archive_script(&archive, event),
            Err(e) => {
                println!("Error on received event: {:?}", e);
                Ok(())
            }
        };
    }

    Ok(())
}


fn main() {

    let matches = App::new("SArchive")
        .version("0.1.0")
        .author("Andy Georges <itkovian+sarchive@gmail.com>")
        .about("Archive slurm user job scripts")
        .arg(Arg::with_name("archive")
            .long("archive")
            .short("a")
            .takes_value(true)
            .help("Location of the job scripts' archive.")
        )
        .arg(Arg::with_name("cluster")
            .long("cluster")
            .short("c")
            .takes_value(true)
            .help("Name of the cluster where the jobs have been submitted to")
        )
        .arg(Arg::with_name("spool")
            .long("spool")
            .short("s")
            .takes_value(true)
            .help("Location of the Slurm StateSaveLocation (where the job hash dirs are kept)")
        )
        .get_matches();

    let base = Path::new(matches.value_of("spool").unwrap());
    let archive = Path::new(matches.value_of("archive").unwrap());

    // TODO: check the base exists

    watch_and_copy(&archive, &base, 0);
}
