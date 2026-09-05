use std::fs;
use std::io::{BufWriter, Write};
use std::path::PathBuf;

pub fn write_lines(
    path: &PathBuf,
    lines: Vec<String>,
    append: bool,
) -> Result<(), crate::error::DecSyncError> {
    let mut options = fs::OpenOptions::new();
    options.create(true);
    if append {
        options.append(true);
    } else {
        options.write(true).truncate(true);
    }

    let file = options.open(path)?;
    let mut file = BufWriter::new(file);

    for line in lines {
        writeln!(file, "{}", line)?;
    }

    file.flush()?;

    Ok(())
}

pub fn read_lines(path: &PathBuf) -> Result<Vec<String>, crate::error::DecSyncError> {
    Ok(fs::read_to_string(path)
        .map(|text| text.lines().map(String::from).collect())
        .unwrap_or_default())
}
