extern crate clap;
extern crate notify;

use clap::{Arg, App};
//use crossbeam_channel::{unbounded};
use notify::{DebouncedEvent, RecommendedWatcher, RecursiveMode, Watcher};
use std::fs::{copy};
use std::io::{Error};
use std::path::{Path};
use std::sync::mpsc::channel;
use std::time::Duration;


fn is_job_path(path: &Path) -> Option<(&str, &str)> {
    // e.g., /var/spool/slurm/hash.1/job.0021/script
    // what we need is the job ID (0021) and the name of the actual file (script)
    if path.is_file() {
        let file = path.file_name().unwrap().to_str().unwrap();
        let parent = path.parent().unwrap();
        let jobname = parent.file_name().unwrap().to_str().unwrap();
        
        if jobname.starts_with("job.") {
            return Some((parent.extension().unwrap().to_str().unwrap(), file))
        };
    }
    None
}


fn archive_script(archive: &Path, event: DebouncedEvent) -> Result<(), Error> {
    println!("Event received: {:?}", event);
    match event {
        DebouncedEvent::Create(path) => {
            if let Some((jobid, job_filename)) = is_job_path(&path) {
                let target_path = archive.join(format!("job.{}_{}", &jobid, &job_filename));
                match copy(&path, &target_path) {
                    Ok(bytes) => { 
                        println!("{} bytes copied to {:?}", bytes, &target_path)
                    },
                    Err(e)    => { 
                        println!("Copy of {:?} to {:?} failed: {:?}", &path, &target_path, e);
                        return Err(e);
                    }
                };
            };
        },
        // We ignore all other events
        _ => ()
    }
    Ok(())
}


fn watch_and_copy(archive: &Path, base: &Path, hash: u8) -> notify::Result<()> {

    let (tx, rx) = channel();

    // create a platform-specific watcher
    let mut watcher:RecommendedWatcher = Watcher::new(tx, Duration::from_secs(2))?;  
    let path = base.join(format!("hash.{}", hash));

    // TODO: check the path exists!

    watcher.watch(&path, RecursiveMode::Recursive)?;

    loop {
        match rx.recv() {
            Ok(event) => archive_script(&archive, event)?,
            Err(e) => {
                println!("Error on received event: {:?}", e);
                break; 
            }
        };
    }

    Ok(())
}


fn main() {

    let matches = App::new("SArchive")
        .version("0.1.0")
        .author("Andy Georges <itkovian+sarchive@gmail.com>")
        .about("Archive slurm user job scripts.")
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
            .help("Name of the cluster where the jobs have been submitted to.")
        )
        .arg(Arg::with_name("spool")
            .long("spool")
            .short("s")
            .takes_value(true)
            .help("Location of the Slurm StateSaveLocation (where the job hash dirs are kept).")
        )
        .get_matches();

    let base = Path::new(matches.value_of("spool").unwrap());
    let archive = Path::new(matches.value_of("archive").unwrap());

    // TODO: check the base exists

    watch_and_copy(&archive, &base, 0);
}
