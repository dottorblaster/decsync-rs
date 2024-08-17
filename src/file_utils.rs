use std::fs;
use std::io::{BufWriter, Write};
use std::path::PathBuf;

pub fn write_lines(
    path: &PathBuf,
    lines: Vec<String>,
    append: bool,
) -> Result<(), crate::error::DecSyncError> {
    let file = fs::OpenOptions::new()
        .create(true)
        .append(append)
        .open(&path)?;
    let mut file = BufWriter::new(file);

    for line in lines {
        writeln!(file, "{}", line)?;
    }

    file.flush()?;

    Ok(())
}

pub fn read_lines(path: PathBuf) -> Result<Vec<String>, crate::error::DecSyncError> {
    todo!()
}
