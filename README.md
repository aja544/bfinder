# bfinder

I built to this Rust tool to be fast, parallel, deterministic to find the top-N largest files on Linux.

## Features
- Parallel directory traversal
- Online top-N selection (no full sort)
- Uses `statx`, `openat`, `readdir`
- Faster than `find | sort`

## Usage
```bash
cargo run --release -- /home -n 10
