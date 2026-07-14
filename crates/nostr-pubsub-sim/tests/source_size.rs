use std::fs;
use std::path::{Path, PathBuf};

const MAX_RUST_SOURCE_LINES: usize = 1_000;

#[test]
fn rust_source_files_stay_reviewable() {
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("sim crate must be inside the workspace");
    let mut sources = Vec::new();
    collect_rust_sources(&workspace.join("crates"), &mut sources);
    sources.sort();

    let oversized = sources
        .into_iter()
        .filter_map(|path| {
            let contents = fs::read_to_string(&path).expect("Rust source must be readable");
            let line_count = contents.lines().count();
            (line_count > MAX_RUST_SOURCE_LINES).then(|| {
                let relative = path.strip_prefix(workspace).unwrap_or(&path);
                format!("{}: {line_count} lines", relative.display())
            })
        })
        .collect::<Vec<_>>();

    assert!(
        oversized.is_empty(),
        "Rust source files must not exceed {MAX_RUST_SOURCE_LINES} lines:\n{}",
        oversized.join("\n")
    );
}

fn collect_rust_sources(directory: &Path, sources: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(directory).expect("source directory must be readable") {
        let entry = entry.expect("source entry must be readable");
        let path = entry.path();
        if path.is_dir() {
            collect_rust_sources(&path, sources);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            sources.push(path);
        }
    }
}
