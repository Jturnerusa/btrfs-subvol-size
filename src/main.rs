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

type SubvolId = u64;
type Extent = (u64, u64);

#[derive(Debug)]
struct Error {
    message: String,
    source: Option<Box<dyn std::error::Error + Send>>,
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

            if let Some(s) = e.source {
                eprintln!("caused by: {s}")
            }
        }
    }
}

fn run() -> Result<(), Error> {
    let args = Args::parse();

    let root_file = File::open(args.root.as_path()).map_err(|e| Error {
        message: "failed to open root".to_string(),
        source: Some(Box::new(e)),
    })?;

    let mut subvols: HashMap<SubvolId, HashSet<Extent>> = HashMap::new();

    for root_item in TreeSearch::search_all(&root_file, Tree::Root) {
        match root_item {
            Ok((key, Item::RootBackRef(root))) => {
                eprintln!(
                    "collecting extents for subvol {}",
                    root.name.as_path().to_str().unwrap_or("?")
                );

                for item in TreeSearch::search_all(&root_file, Tree::Subvol(key.objectid)) {
                    match item {
                        Ok((_, Item::FileExtentReg(extent))) => {
                            subvols
                                .entry(key.objectid)
                                .or_default()
                                .insert((extent.disk_bytenr.get(), extent.num_bytes.get()));
                        }
                        Ok(_) => continue,
                        Err(e) => {
                            return Err(Error {
                                message: format!(
                                    "failed to walk subvolume tree: {}",
                                    root.name.as_path().to_str().unwrap_or("?")
                                ),
                                source: Some(Box::new(e)),
                            })
                        }
                    }
                }
            }
            Ok(_) => continue,
            Err(e) => {
                return Err(Error {
                    message: "failed to walk root tree".to_string(),
                    source: Some(Box::new(e)),
                })
            }
        }
    }

    subvols.par_iter().try_for_each(|(id, extents)| {
        let other = subvols
            .iter()
            .filter_map(|(s, e)| {
                if s == id {
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

        let name = match get_subvol_name(*id, &root_file) {
            Ok(Some(name)) => name,
            Ok(None) => {
                return Err(Error {
                    message: "failed to find name for subvolume {id}".to_string(),
                    source: None,
                })
            }
            Err(e) => return Err(e),
        };

        println!(
            "{total} {exclusive} {}",
            name.as_path().to_str().unwrap_or("?")
        );

        Ok(())
    })?;

    Ok(())
}

fn get_subvol_name(id: u64, root: &File) -> Result<Option<PathBuf>, Error> {
    for item in TreeSearch::search_all(root, Tree::Root) {
        match item {
            Ok((key, Item::RootBackRef(root))) if key.objectid == id => {
                return Ok(Some(root.name.clone()))
            }
            Ok(_) => continue,
            Err(e) => {
                return Err(Error {
                    message: "failed to walk root tree".to_string(),
                    source: Some(Box::new(e)),
                })
            }
        }
    }

    Ok(None)
}
