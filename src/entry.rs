//! Types that cross the public API and the on-disk format.

use serde_json::{json, Value};

#[derive(Debug, Clone)]
/// One entry: a timestamp, a key and a value.
///
/// Datetimes are fixed-width UTC strings (`YYYY-MM-DDTHH:MM:SS`), so
/// `String` ordering equals chronological order and other DecSync
/// implementations agree on what "newer" means.
pub struct Entry {
    /// When the entry was written, `YYYY-MM-DDTHH:MM:SS` UTC.
    pub datetime: String,
    /// Arbitrary JSON value identifying the entry inside its path.
    pub key: Value,
    /// Arbitrary JSON value.
    pub value: Value,
}

impl Entry {
    /// Builds an entry from parts.
    pub fn new(datetime: String, key: Value, value: Value) -> Entry {
        Entry {
            datetime,
            key,
            value,
        }
    }

    /// Serializes to the JSON array `[datetime, key, value]`.
    pub fn to_json(&self) -> Value {
        json!([self.datetime, self.key, self.value])
    }
}

/// Parses the JSON array `[datetime, key, value]` from one line. A
/// malformed line is [`DecSyncError::InvalidEntry`](crate::error::DecSyncError::InvalidEntry).
impl TryFrom<String> for Entry {
    type Error = crate::error::DecSyncError;

    fn try_from(line: String) -> Result<Self, Self::Error> {
        let (datetime, key, value): (String, Value, Value) = serde_json::from_str(&line)?;
        Ok(Self::new(datetime, key, value))
    }
}

#[derive(Debug, Clone)]
/// An [`Entry`] plus the path it lives under; one unit of the v2
/// format, serialized as `[path, datetime, key, value]`.
pub struct EntryWithPath {
    /// The full path of the entry.
    pub path: Vec<String>,
    /// The entry itself.
    pub entry: Entry,
}

impl EntryWithPath {
    /// Builds an entry with path from parts.
    pub fn new(path: Vec<String>, datetime: String, key: Value, value: Value) -> EntryWithPath {
        EntryWithPath {
            path,
            entry: Entry {
                datetime,
                key,
                value,
            },
        }
    }

    /// Serializes to the JSON array `[path, datetime, key, value]`.
    pub fn to_json(&self) -> Value {
        json!([
            self.path,
            self.entry.datetime,
            self.entry.key,
            self.entry.value
        ])
    }
}

/// Parses the JSON array `[path, datetime, key, value]` from one line.
impl TryFrom<String> for EntryWithPath {
    type Error = crate::error::DecSyncError;

    fn try_from(line: String) -> Result<Self, Self::Error> {
        let (path, datetime, key, value): (Vec<String>, String, Value, Value) =
            serde_json::from_str(&line)?;
        Ok(Self::new(path, datetime, key, value))
    }
}

pub(crate) struct GroupedEntries {
    pub hash: String,
    pub entries: Vec<EntryWithPath>,
}

/// A path and key whose value is still on disk.
///
/// Used to ask for retroactive execution of stored entries whose
/// value the caller does not know.
pub struct StoredEntry {
    /// Where to look.
    pub path: Vec<String>,
    /// Which key to execute.
    pub key: Value,
}

impl StoredEntry {
    /// Builds a stored entry from parts.
    pub fn new(path: Vec<String>, key: Value) -> StoredEntry {
        StoredEntry { path, key }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::time;
    use serde_json::json;

    #[test]
    fn entry_is_constructed() {
        let now = time::current_date_time();
        let entry = Entry::new(now, json!("some_key"), json!({"some": "value"}));

        assert_eq!(entry.key, Value::from("some_key"));
    }

    #[test]
    fn to_json() {
        let now = time::current_date_time();
        let entry = Entry::new(now, json!("some_key"), json!({"some": "value"}));

        let json = entry.to_json();
        assert_eq!(json.get(1).unwrap(), &Value::from("some_key"));
    }

    #[test]
    fn entry_from_json() {
        let serialized_entry = "[\"kekw\", \"some_key\", 2]".to_owned();

        let entry = Entry::try_from(serialized_entry).unwrap();

        assert_eq!(entry.key, Value::from("some_key"));
    }

    #[test]
    fn entry_with_path_from_json() {
        let serialized_entry = "[[\"/usr/share\"], \"kekw\", \"some_key\", 2]".to_owned();

        let entry = EntryWithPath::try_from(serialized_entry).unwrap();

        assert_eq!(entry.path, vec!("/usr/share"));
        assert_eq!(entry.entry.key, Value::from("some_key"));
    }
}
