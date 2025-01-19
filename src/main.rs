use core::{fmt, option::Option};
use std::{
    collections::{HashMap, HashSet},
    fs::File,
    path::PathBuf,
};

use clap::Parser;
use libbtrfsrs::{
    tree_search::{Item, Tree},
    TreeSearch,
};
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};

#[derive(Debug)]
struct Error {
    message: String,
    source: Option<Box<dyn std::error::Error>>,
}

#[derive(clap::Parser)]
struct Args {
    #[arg(long)]
    root: PathBuf,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for Error {}

fn main() {
    match run() {
        Ok(_) => (),
        Err(e) => {
            eprintln!("{e}");
        }
    }
}

fn run() -> Result<(), Error> {
    let args = Args::parse();

    let root_file = File::open(args.root.as_path()).map_err(|e| Error {
        message: "failed to open root".to_string(),
        source: Some(Box::new(e)),
    })?;

    let mut subvols: HashMap<PathBuf, HashSet<(u64, u64)>> = HashMap::new();

    for root_item in TreeSearch::search_all(&root_file, Tree::Root) {
        match root_item {
            Ok((key, Item::RootBackRef(root))) => {
                eprintln!(
                    "collecting extents for subvol {}",
                    root.name.as_path().to_str().unwrap_or("?")
                );

                for item in TreeSearch::search_all(&root_file, Tree::Subvol(key.objectid)) {
                    match item {
                        Ok((key, Item::FileExtentReg(extent))) => {
                            subvols
                                .entry(root.name.clone())
                                .or_default()
                                .insert((extent.disk_bytenr.get(), extent.num_bytes.get()));
                        }
                        _ => (),
                    }
                }
            }
            _ => (),
        }
    }

    subvols.par_iter().for_each(|(subvol, extents)| {
        let other = subvols
            .iter()
            .filter_map(|(s, e)| {
                if s == subvol {
                    None
                } else {
                    Some(e.iter().copied())
                }
            })
            .flatten()
            .collect::<HashSet<_, _>>();

        let difference = extents - &other;

        let total = extents.iter().map(|(_, num_bytes)| *num_bytes).sum::<u64>();
        let exclusive = difference
            .iter()
            .map(|(_, num_bytes)| *num_bytes)
            .sum::<u64>();

        println!(
            "{total} {exclusive} {}",
            subvol.as_path().to_str().unwrap_or("?")
        );
    });

    Ok(())
}
