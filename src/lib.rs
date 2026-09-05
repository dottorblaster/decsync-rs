use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::path::PathBuf;

mod entry;
mod error;
mod file_utils;
mod hash;
mod time;

pub struct DecSyncInstance {
    pub decsync_dir: String,
    pub sync_type: String,
    pub collection: Option<String>,
    pub own_app_id: String,
    decsync_dir_path: PathBuf,
}

impl DecSyncInstance {
    pub fn new(
        decsync_dir: String,
        sync_type: String,
        collection: Option<String>,
        own_app_id: String,
    ) -> DecSyncInstance {
        let decsync_dir_path = get_decsync_subdir(&decsync_dir, &sync_type, &collection);
        let _ = fs::create_dir_all(decsync_dir_path.join("v2").join(&own_app_id));
        let _ = fs::create_dir_all(decsync_dir_path.join("local").join(&own_app_id));

        DecSyncInstance {
            decsync_dir,
            sync_type,
            collection,
            own_app_id,
            decsync_dir_path,
        }
    }

    fn own_v2_dir(&self) -> PathBuf {
        self.decsync_dir_path.join("v2").join(&self.own_app_id)
    }

    fn sequences_file(&self) -> PathBuf {
        self.own_v2_dir().join("sequences")
    }

    pub fn set_entry(
        &self,
        path: Vec<String>,
        key: serde_json::Value,
        value: serde_json::Value,
    ) -> Result<(), error::DecSyncError> {
        self.set_entries(vec![entry::EntryWithPath::new(
            path,
            time::current_date_time(),
            key,
            value,
        )])
    }

    pub fn set_entries(
        &self,
        entries_with_path: Vec<entry::EntryWithPath>,
    ) -> Result<(), error::DecSyncError> {
        if entries_with_path.is_empty() {
            return Ok(());
        }

        let sequences_file = self.sequences_file();
        let mut sequences = read_sequences(&sequences_file)?;

        for group in group_by_hash(entries_with_path) {
            let file = self.own_v2_dir().join(&group.hash);
            let new_entries = update_entries(&file, group.entries, true)?;
            if !new_entries.is_empty() {
                *sequences.entry(group.hash).or_insert(0) += 1;
            }
        }

        write_sequences(&sequences_file, &sequences)
    }
}

fn group_by_hash(entries_with_path: Vec<entry::EntryWithPath>) -> Vec<entry::GroupedEntries> {
    let mut groups: BTreeMap<String, Vec<entry::EntryWithPath>> = BTreeMap::new();
    for entry_with_path in entries_with_path {
        let hash = hash::path_to_hash(entry_with_path.path.clone());
        groups.entry(hash).or_default().push(entry_with_path);
    }
    groups
        .into_iter()
        .map(|(hash, entries)| entry::GroupedEntries { hash, entries })
        .collect()
}

fn update_entries(
    file: &PathBuf,
    entries_with_path: Vec<entry::EntryWithPath>,
    require_new_value: bool,
) -> Result<Vec<entry::EntryWithPath>, error::DecSyncError> {
    let stored_entries = read_stored_entries(file);

    let new_entries = entries_with_path
        .into_iter()
        .filter(|entry_with_path| {
            stored_entries
                .get(&(
                    entry_with_path.path.clone(),
                    entry_with_path.entry.key.clone(),
                ))
                .is_none_or(|stored| {
                    entry_with_path.entry.datetime > stored.entry.datetime
                        && !(require_new_value && entry_with_path.entry.value == stored.entry.value)
                })
        })
        .collect::<Vec<_>>();

    if !new_entries.is_empty() {
        let new_keys = new_entries
            .iter()
            .map(|entry_with_path| {
                (
                    entry_with_path.path.clone(),
                    entry_with_path.entry.key.clone(),
                )
            })
            .collect::<HashSet<_>>();

        let lines = stored_entries
            .into_iter()
            .filter(|(key, _)| !new_keys.contains(key))
            .map(|(_, entry_with_path)| entry_with_path.to_json().to_string())
            .chain(
                new_entries
                    .iter()
                    .map(|entry_with_path| entry_with_path.to_json().to_string()),
            )
            .collect();

        file_utils::write_lines(file, lines, false)?;
    }

    Ok(new_entries)
}

fn read_stored_entries(
    file: &PathBuf,
) -> HashMap<(Vec<String>, serde_json::Value), entry::EntryWithPath> {
    file_utils::read_lines(file)
        .unwrap_or_default()
        .into_iter()
        .filter_map(|line| entry::EntryWithPath::try_from(line).ok())
        .map(|entry_with_path| {
            (
                (
                    entry_with_path.path.clone(),
                    entry_with_path.entry.key.clone(),
                ),
                entry_with_path,
            )
        })
        .collect()
}

fn read_sequences(path: &PathBuf) -> Result<BTreeMap<String, i64>, error::DecSyncError> {
    Ok(fs::read_to_string(path)
        .ok()
        .and_then(|text| serde_json::from_str(&text).ok())
        .unwrap_or_default())
}

fn write_sequences(
    path: &PathBuf,
    sequences: &BTreeMap<String, i64>,
) -> Result<(), error::DecSyncError> {
    file_utils::write_lines(path, vec![serde_json::to_string(sequences)?], false)
}

fn get_decsync_subdir(decsync_dir: &str, sync_type: &str, collection: &Option<String>) -> PathBuf {
    let mut decsync_path = PathBuf::new();

    decsync_path.push(decsync_dir);
    decsync_path.push(sync_type);

    if let Some(collection_path) = collection {
        decsync_path.push(collection_path);
    }

    decsync_path
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("decsync-test-{}", name));
        let _ = fs::remove_dir_all(&dir);
        dir
    }

    fn instance(dir: &PathBuf) -> DecSyncInstance {
        DecSyncInstance::new(
            dir.to_string_lossy().into_owned(),
            "rss".to_owned(),
            None,
            "app1".to_owned(),
        )
    }

    #[test]
    fn decsync_is_constructed() {
        let dir = test_dir("constructed");
        let decsync_instance = instance(&dir);
        assert_eq!(decsync_instance.own_app_id, "app1".to_owned());
        assert!(fs::metadata(decsync_instance.own_v2_dir()).is_ok());
        assert!(fs::metadata(decsync_instance.decsync_dir_path.join("local").join("app1")).is_ok());
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn set_entry_writes_entry_file_and_sequences() {
        let dir = test_dir("set-entry");
        let decsync = instance(&dir);

        decsync
            .set_entry(
                vec!["feeds".to_owned(), "subscriptions".to_owned()],
                serde_json::json!("https://foo.example.com/rss"),
                serde_json::json!(true),
            )
            .unwrap();

        let content = fs::read_to_string(decsync.own_v2_dir().join("b9")).unwrap();
        let lines = content.lines().collect::<Vec<_>>();
        assert_eq!(lines.len(), 1);
        let parsed: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(parsed[0], serde_json::json!(["feeds", "subscriptions"]));
        assert!(parsed[1].is_string());
        assert_eq!(parsed[2], serde_json::json!("https://foo.example.com/rss"));
        assert_eq!(parsed[3], serde_json::json!(true));

        let sequences: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(decsync.sequences_file()).unwrap()).unwrap();
        assert_eq!(sequences, serde_json::json!({"b9": 1}));
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn updating_existing_key_replaces_and_bumps_sequence() {
        let dir = test_dir("update-key");
        let decsync = instance(&dir);
        let path = vec!["feeds".to_owned(), "subscriptions".to_owned()];

        decsync
            .set_entries(vec![entry::EntryWithPath::new(
                path.clone(),
                "2020-07-17T12:30:00".to_owned(),
                serde_json::json!("url"),
                serde_json::json!(true),
            )])
            .unwrap();
        decsync
            .set_entries(vec![entry::EntryWithPath::new(
                path,
                "2020-07-17T12:40:00".to_owned(),
                serde_json::json!("url"),
                serde_json::json!(false),
            )])
            .unwrap();

        let content = fs::read_to_string(decsync.own_v2_dir().join("b9")).unwrap();
        let lines = content.lines().collect::<Vec<_>>();
        assert_eq!(lines.len(), 1);
        let parsed: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(parsed[1], serde_json::json!("2020-07-17T12:40:00"));
        assert_eq!(parsed[3], serde_json::json!(false));

        let sequences: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(decsync.sequences_file()).unwrap()).unwrap();
        assert_eq!(sequences, serde_json::json!({"b9": 2}));
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn stale_update_is_ignored() {
        let dir = test_dir("stale");
        let decsync = instance(&dir);
        let path = vec!["feeds".to_owned(), "subscriptions".to_owned()];

        decsync
            .set_entries(vec![entry::EntryWithPath::new(
                path.clone(),
                "2020-07-17T12:40:00".to_owned(),
                serde_json::json!("url"),
                serde_json::json!(false),
            )])
            .unwrap();
        decsync
            .set_entries(vec![entry::EntryWithPath::new(
                path,
                "2020-07-17T12:30:00".to_owned(),
                serde_json::json!("url"),
                serde_json::json!(true),
            )])
            .unwrap();

        let content = fs::read_to_string(decsync.own_v2_dir().join("b9")).unwrap();
        assert_eq!(content.lines().count(), 1);
        let sequences: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(decsync.sequences_file()).unwrap()).unwrap();
        assert_eq!(sequences, serde_json::json!({"b9": 1}));
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn same_value_is_not_rewritten() {
        let dir = test_dir("same-value");
        let decsync = instance(&dir);
        let path = vec!["feeds".to_owned(), "subscriptions".to_owned()];

        decsync
            .set_entries(vec![entry::EntryWithPath::new(
                path.clone(),
                "2020-07-17T12:30:00".to_owned(),
                serde_json::json!("url"),
                serde_json::json!(true),
            )])
            .unwrap();
        decsync
            .set_entries(vec![entry::EntryWithPath::new(
                path,
                "2020-07-17T12:40:00".to_owned(),
                serde_json::json!("url"),
                serde_json::json!(true),
            )])
            .unwrap();

        let content = fs::read_to_string(decsync.own_v2_dir().join("b9")).unwrap();
        assert_eq!(content.lines().count(), 1);
        let sequences: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(decsync.sequences_file()).unwrap()).unwrap();
        assert_eq!(sequences, serde_json::json!({"b9": 1}));
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn multiple_entries_same_hash_bump_once() {
        let dir = test_dir("multi-same-hash");
        let decsync = instance(&dir);

        decsync
            .set_entries(vec![
                entry::EntryWithPath::new(
                    vec!["feeds".to_owned(), "subscriptions".to_owned()],
                    "2020-07-17T12:30:00".to_owned(),
                    serde_json::json!("a"),
                    serde_json::json!(true),
                ),
                entry::EntryWithPath::new(
                    vec!["feeds".to_owned(), "subscriptions".to_owned()],
                    "2020-07-17T12:31:00".to_owned(),
                    serde_json::json!("b"),
                    serde_json::json!(true),
                ),
            ])
            .unwrap();

        let content = fs::read_to_string(decsync.own_v2_dir().join("b9")).unwrap();
        assert_eq!(content.lines().count(), 2);
        let sequences: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(decsync.sequences_file()).unwrap()).unwrap();
        assert_eq!(sequences, serde_json::json!({"b9": 1}));
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn entries_in_different_hashes_bump_separately() {
        let dir = test_dir("multi-hash");
        let decsync = instance(&dir);

        decsync
            .set_entries(vec![
                entry::EntryWithPath::new(
                    vec!["feeds".to_owned(), "subscriptions".to_owned()],
                    "2020-07-17T12:30:00".to_owned(),
                    serde_json::json!("a"),
                    serde_json::json!(true),
                ),
                entry::EntryWithPath::new(
                    vec![
                        "entries".to_owned(),
                        "read".to_owned(),
                        "2020".to_owned(),
                        "09".to_owned(),
                        "10".to_owned(),
                    ],
                    "2020-07-17T12:31:00".to_owned(),
                    serde_json::json!("guid"),
                    serde_json::json!(true),
                ),
            ])
            .unwrap();

        let sequences: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(decsync.sequences_file()).unwrap()).unwrap();
        assert_eq!(sequences, serde_json::json!({"b8": 1, "b9": 1}));
        let _ = fs::remove_dir_all(dir);
    }
}
