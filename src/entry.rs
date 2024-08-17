use serde_json::{json, Value};

#[derive(Debug, Clone)]
pub struct Entry {
    pub datetime: String,
    pub key: Value,
    pub value: Value,
}

impl Entry {
    pub fn new(datetime: String, key: Value, value: Value) -> Entry {
        Entry {
            datetime,
            key,
            value,
        }
    }

    pub fn to_json(&self) -> Value {
        json!([self.datetime, self.key, self.value])
    }
}

impl TryFrom<String> for Entry {
    type Error = crate::error::DecSyncError;

    fn try_from(line: String) -> Result<Self, Self::Error> {
        let (datetime, key, value): (String, Value, Value) = serde_json::from_str(&line)?;
        Ok(Self::new(datetime, key, value))
    }
}

#[derive(Debug, Clone)]
pub struct EntryWithPath {
    pub path: Vec<String>,
    pub entry: Entry,
}

impl EntryWithPath {
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

    pub fn to_json(&self) -> Value {
        json!([
            self.path,
            self.entry.datetime,
            self.entry.key,
            self.entry.value
        ])
    }
}

impl TryFrom<String> for EntryWithPath {
    type Error = crate::error::DecSyncError;

    fn try_from(line: String) -> Result<Self, Self::Error> {
        let (path, datetime, key, value): (Vec<String>, String, Value, Value) =
            serde_json::from_str(&line)?;
        Ok(Self::new(path, datetime, key, value))
    }
}

pub struct GroupedEntries {
    pub hash: String,
    pub entries: Vec<EntryWithPath>,
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
    fn entry_with_path_from_json() {
        let serialized_entry = "[[\"/usr/share\"], \"kekw\", \"some_key\", 2]".to_owned();

        let entry = EntryWithPath::try_from(serialized_entry).unwrap();

        assert_eq!(entry.path, vec!("/usr/share"));
        assert_eq!(entry.entry.key, Value::from("some_key"));
    }
}
