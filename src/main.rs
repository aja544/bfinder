use clap::Parser;
use rayon::prelude::*;
use rustix::fs::{statat, AtFlags, FileType, Mode};
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

#[derive(Parser)]
#[command(name = "bfinder")]
#[command(about = "Find the top N largest files with deterministic parallel scanning")]
struct Cli {
    /// Number of largest files to find
    #[arg(short = 'n', long, default_value = "10")]
    top: usize,

    /// Directory to scan
    #[arg(default_value = ".")]
    path: PathBuf,

    /// Number of threads to use (default: number of CPUs)
    #[arg(short = 'j', long)]
    threads: Option<usize>,
}

/// Represents a file with its size and path for deterministic ordering
#[derive(Debug, Clone, Eq, PartialEq)]
struct FileEntry {
    size: u64,
    path: PathBuf,
}

impl Ord for FileEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Total ordering: size descending, then path ascending for determinism
        // When wrapped in Reverse for min-heap, this gives us largest files
        self.size
            .cmp(&other.size)
            .then_with(|| self.path.cmp(&other.path))
    }
}

impl PartialOrd for FileEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Per-thread min-heap for tracking top-N candidates
struct TopNHeap {
    heap: BinaryHeap<Reverse<FileEntry>>,
    capacity: usize,
}

impl TopNHeap {
    fn new(capacity: usize) -> Self {
        Self {
            heap: BinaryHeap::with_capacity(capacity + 1),
            capacity,
        }
    }

    fn insert(&mut self, entry: FileEntry) {
        self.heap.push(Reverse(entry));
        if self.heap.len() > self.capacity {
            self.heap.pop();
        }
    }

    fn into_vec(self) -> Vec<FileEntry> {
        self.heap.into_iter().map(|Reverse(e)| e).collect()
    }
}

/// Scanner statistics
#[derive(Default)]
struct ScanStats {
    files_scanned: u64,
    dirs_scanned: u64,
    errors: u64,
}

/// Single-observation directory entry with metadata
struct DirEntry {
    name: String,
    path: PathBuf,
}

/// Scan a single directory atomically: read entries once, sort lexicographically,
/// classify each with a single statx() call
fn scan_directory(
    dir_path: &Path,
    top_n: &mut TopNHeap,
    stats: &mut ScanStats,
    subdirs: &mut Vec<PathBuf>,
) -> io::Result<()> {
    // Read directory entries exactly once
    let mut entries: Vec<DirEntry> = Vec::new();

    for entry in fs::read_dir(dir_path)? {
        let entry = match entry {
            Ok(e) => e,
            Err(_) => {
                stats.errors += 1;
                continue; // Skip entries we can't read, never retry
            }
        };

        let name = entry.file_name();
        let name_str = match name.to_str() {
            Some(s) => s.to_string(),
            None => {
                stats.errors += 1;
                continue; // Skip non-UTF8 names
            }
        };

        entries.push(DirEntry {
            name: name_str,
            path: entry.path(),
        });
    }

    // Sort entries lexicographically for deterministic traversal order
    entries.sort_by(|a, b| a.name.cmp(&b.name));

    // Classify each entry exactly once with single statx() call
    for entry in entries {
        // Single classification attempt - never retry
        let metadata = match classify_entry(dir_path, &entry.name) {
            Ok(m) => m,
            Err(_) => {
                stats.errors += 1;
                continue; // Failed classification, skip this entry
            }
        };

        match metadata {
            EntryMetadata::RegularFile { size } => {
                stats.files_scanned += 1;
                top_n.insert(FileEntry {
                    size,
                    path: entry.path,
                });
            }
            EntryMetadata::Directory => {
                stats.dirs_scanned += 1;
                subdirs.push(entry.path);
            }
            EntryMetadata::Other => {
                // Symlinks, devices, etc. - ignore
            }
        }
    }

    Ok(())
}

/// Entry classification result
enum EntryMetadata {
    RegularFile { size: u64 },
    Directory,
    Other,
}

/// Classify an entry with a single statx() call, using d_type as hint but not guarantee
fn classify_entry(parent: &Path, name: &str) -> io::Result<EntryMetadata> {
    // Open parent directory for *at operations
    let parent_fd = rustix::fs::openat(
        rustix::fs::CWD,
        parent,
        rustix::fs::OFlags::RDONLY | rustix::fs::OFlags::DIRECTORY | rustix::fs::OFlags::CLOEXEC,
        Mode::empty(),
    )?;

    // Single statx() call - never retry, don't follow symlinks
    let stat = statat(
        &parent_fd,
        name,
        AtFlags::SYMLINK_NOFOLLOW,
    )?;

    let file_type = FileType::from_raw_mode(stat.st_mode as rustix::fs::RawMode);

    let result = if file_type == FileType::RegularFile {
        EntryMetadata::RegularFile {
            size: stat.st_size as u64,
        }
    } else if file_type == FileType::Directory {
        EntryMetadata::Directory
    } else {
        EntryMetadata::Other
    };

    Ok(result)
}

/// Parallel directory traversal using level-by-level BFS
fn parallel_scan(root: PathBuf, capacity: usize) -> (Vec<FileEntry>, ScanStats) {
    let global_stats = Mutex::new(ScanStats::default());
    let thread_heaps = Mutex::new(Vec::<TopNHeap>::new());

    // Work queue of directories to process
    let mut work_queue = vec![root];

    while !work_queue.is_empty() {
        // Next level queue wrapped in Mutex for parallel access
        let next_queue = Mutex::new(Vec::new());

        // Process current level of directories in parallel
        let results: Vec<_> = work_queue
            .par_iter()
            .map_init(
                || (TopNHeap::new(capacity), ScanStats::default()),
                |(top_n, stats), dir| {
                    let mut subdirs = Vec::new();

                    // Scan this directory atomically
                    if let Err(_) = scan_directory(dir, top_n, stats, &mut subdirs) {
                        stats.errors += 1;
                    }

                    // Add subdirectories to next level (synchronized)
                    if !subdirs.is_empty() {
                        next_queue.lock().unwrap().extend(subdirs);
                    }

                    // Return ownership of heap and stats
                    (std::mem::replace(top_n, TopNHeap::new(capacity)),
                     std::mem::take(stats))
                },
            )
            .collect();

        // Collect per-thread heaps and stats
        for (heap, stats) in results {
            thread_heaps.lock().unwrap().push(heap);
            let mut global = global_stats.lock().unwrap();
            global.files_scanned += stats.files_scanned;
            global.dirs_scanned += stats.dirs_scanned;
            global.errors += stats.errors;
        }

        // Move to next level
        work_queue = next_queue.into_inner().unwrap();
    }

    // Merge all thread heaps deterministically
    let heaps = thread_heaps.into_inner().unwrap();
    let merged = merge_heaps(heaps, capacity);
    let stats = global_stats.into_inner().unwrap();

    (merged, stats)
}

/// Merge per-thread heaps into final top-N with total ordering
fn merge_heaps(heaps: Vec<TopNHeap>, capacity: usize) -> Vec<FileEntry> {
    let mut final_heap = TopNHeap::new(capacity);

    for heap in heaps {
        for entry in heap.into_vec() {
            final_heap.insert(entry);
        }
    }

    let mut results = final_heap.into_vec();
    // Sort in descending order (largest first) with path as tiebreaker
    results.sort_by(|a, b| {
        b.size
            .cmp(&a.size)
            .then_with(|| a.path.cmp(&b.path))
    });
    results
}

/// Format file size in human-readable format
fn format_size(size: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;

    if size >= GB {
        format!("{:.2} GB", size as f64 / GB as f64)
    } else if size >= MB {
        format!("{:.2} MB", size as f64 / MB as f64)
    } else if size >= KB {
        format!("{:.2} KB", size as f64 / KB as f64)
    } else {
        format!("{} bytes", size)
    }
}

fn main() {
    let cli = Cli::parse();

    if let Some(threads) = cli.threads {
        rayon::ThreadPoolBuilder::new()
            .num_threads(threads)
            .build_global()
            .unwrap();
    }

    let start = std::time::Instant::now();
    let (results, stats) = parallel_scan(cli.path, cli.top);
    let elapsed = start.elapsed();

    // Output results
    println!("Top {} largest files:", cli.top);
    println!();
    for (i, entry) in results.iter().enumerate() {
        println!(
            "{:4}. {:>12}  {}",
            i + 1,
            format_size(entry.size),
            entry.path.display()
        );
    }

    println!();
    println!("Statistics:");
    println!("  Files scanned:       {}", stats.files_scanned);
    println!("  Directories scanned: {}", stats.dirs_scanned);
    println!("  Errors:              {}", stats.errors);
    println!("  Time elapsed:        {:.3}s", elapsed.as_secs_f64());
}
