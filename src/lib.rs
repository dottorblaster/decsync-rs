use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::path::PathBuf;

mod entry;
mod error;
mod file_utils;
mod hash;
mod time;

pub struct DecSyncInstance<T> {
    pub decsync_dir: String,
    pub sync_type: String,
    pub collection: Option<String>,
    pub own_app_id: String,
    decsync_dir_path: PathBuf,
    listeners: Vec<Listener<T>>,
}

type EntryCallback<T> = Box<dyn FnMut(Vec<String>, &mut Vec<entry::Entry>, &T) -> bool>;

struct Listener<T> {
    subpath: Vec<String>,
    callback: EntryCallback<T>,
}

struct UpdateOutcome {
    entries: Vec<entry::EntryWithPath>,
    success: bool,
}

impl<T> DecSyncInstance<T> {
    pub fn new(
        decsync_dir: String,
        sync_type: String,
        collection: Option<String>,
        own_app_id: String,
    ) -> DecSyncInstance<T> {
        let decsync_dir_path = get_decsync_subdir(&decsync_dir, &sync_type, &collection);
        let _ = fs::create_dir_all(decsync_dir_path.join("v2").join(&own_app_id));
        let _ = fs::create_dir_all(decsync_dir_path.join("local").join(&own_app_id));

        DecSyncInstance {
            decsync_dir,
            sync_type,
            collection,
            own_app_id,
            decsync_dir_path,
            listeners: Vec::new(),
        }
    }

    fn own_v2_dir(&self) -> PathBuf {
        self.decsync_dir_path.join("v2").join(&self.own_app_id)
    }

    fn sequences_file(&self) -> PathBuf {
        self.own_v2_dir().join("sequences")
    }

    fn local_dir(&self) -> PathBuf {
        self.decsync_dir_path.join("local").join(&self.own_app_id)
    }

    pub fn add_listener<F>(&mut self, subpath: Vec<String>, on_entry_update: F)
    where
        F: FnMut(Vec<String>, entry::Entry, &T) + 'static,
    {
        let mut on_entry_update = on_entry_update;
        let callback = Box::new(
            move |path: Vec<String>, entries: &mut Vec<entry::Entry>, extra: &T| {
                let old_entries = std::mem::take(entries);
                let mut survivors = Vec::with_capacity(old_entries.len());
                for entry in old_entries {
                    on_entry_update(path.clone(), entry.clone(), extra);
                    survivors.push(entry);
                }
                *entries = survivors;
                true
            },
        );
        self.listeners.push(Listener { subpath, callback });
    }

    pub fn add_listener_with_success<F>(&mut self, subpath: Vec<String>, on_entry_update: F)
    where
        F: FnMut(Vec<String>, entry::Entry, &T) -> bool + 'static,
    {
        let mut on_entry_update = on_entry_update;
        let callback = Box::new(
            move |path: Vec<String>, entries: &mut Vec<entry::Entry>, extra: &T| {
                let old_entries = std::mem::take(entries);
                let mut all_success = true;
                let mut survivors = Vec::with_capacity(old_entries.len());
                for entry in old_entries {
                    let success = on_entry_update(path.clone(), entry.clone(), extra);
                    all_success &= success;
                    if success {
                        survivors.push(entry);
                    }
                }
                *entries = survivors;
                all_success
            },
        );
        self.listeners.push(Listener { subpath, callback });
    }

    pub fn add_multi_listener<F>(&mut self, subpath: Vec<String>, on_entries_update: F)
    where
        F: FnMut(Vec<String>, &[entry::Entry], &T) + 'static,
    {
        let mut on_entries_update = on_entries_update;
        let callback = Box::new(
            move |path: Vec<String>, entries: &mut Vec<entry::Entry>, extra: &T| {
                on_entries_update(path, entries, extra);
                true
            },
        );
        self.listeners.push(Listener { subpath, callback });
    }

    pub fn add_multi_listener_with_success<F>(&mut self, subpath: Vec<String>, on_entries_update: F)
    where
        F: FnMut(Vec<String>, &[entry::Entry], &T) -> bool + 'static,
    {
        let mut on_entries_update = on_entries_update;
        let callback = Box::new(
            move |path: Vec<String>, entries: &mut Vec<entry::Entry>, extra: &T| {
                let success = on_entries_update(path, entries, extra);
                if !success {
                    entries.clear();
                }
                success
            },
        );
        self.listeners.push(Listener { subpath, callback });
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
            let outcome = update_entries::<T>(None, &file, group.entries, true, None)?;
            if !outcome.entries.is_empty() {
                *sequences.entry(group.hash).or_insert(0) += 1;
            }
        }

        write_sequences(&sequences_file, &sequences)
    }

    pub fn execute_all_new_entries(&mut self, extra: &T) -> Result<(), error::DecSyncError> {
        self.execute_all_new_entries_internal(Some(extra))
    }

    pub fn init_stored_entries(&mut self) -> Result<(), error::DecSyncError> {
        self.execute_all_new_entries_internal(None)
    }

    fn execute_all_new_entries_internal(
        &mut self,
        extra: Option<&T>,
    ) -> Result<(), error::DecSyncError> {
        let v2_dir = self.decsync_dir_path.join("v2");
        let local_sequences_file = self.local_dir().join("sequences");
        let mut local_sequences = read_local_sequences(&local_sequences_file)?;

        let mut updated = false;
        for app_id in file_utils::list_directories(&v2_dir)? {
            if app_id == self.own_app_id {
                continue;
            }

            let app_dir = v2_dir.join(&app_id);
            let app_sequences = read_sequences(&app_dir.join("sequences"))?;
            for (hash, sequence) in app_sequences {
                let already_read = local_sequences
                    .get(&app_id)
                    .and_then(|hashes| hashes.get(&hash))
                    .copied()
                    .unwrap_or(0);
                if already_read == sequence {
                    continue;
                }

                let app_file = app_dir.join(&hash);
                if !app_file.is_file() {
                    continue;
                }

                let own_file = self.own_v2_dir().join(&hash);
                let entries = read_entries(&app_file);
                let outcome = update_entries(
                    Some(&mut self.listeners),
                    &own_file,
                    entries.into_values().collect(),
                    false,
                    extra,
                )?;

                if outcome.success {
                    local_sequences
                        .entry(app_id.clone())
                        .or_default()
                        .insert(hash, sequence);
                    updated = true;
                }
            }
        }

        if updated {
            write_local_sequences(&local_sequences_file, &local_sequences)?;
        }

        Ok(())
    }

    pub fn execute_stored_entry(
        &mut self,
        path: Vec<String>,
        key: serde_json::Value,
        extra: &T,
    ) -> Result<bool, error::DecSyncError> {
        self.execute_stored_entries_for_path_exact(path, extra, Some(std::slice::from_ref(&key)))
    }

    pub fn execute_stored_entries(
        &mut self,
        stored_entries: Vec<entry::StoredEntry>,
        extra: &T,
    ) -> Result<bool, error::DecSyncError> {
        let mut groups: BTreeMap<Vec<String>, Vec<serde_json::Value>> = BTreeMap::new();
        for stored_entry in stored_entries {
            groups
                .entry(stored_entry.path)
                .or_default()
                .push(stored_entry.key);
        }

        let mut all_success = true;
        for (path, keys) in groups {
            all_success &= self.execute_stored_entries_for_path_exact(path, extra, Some(&keys))?;
        }
        Ok(all_success)
    }

    pub fn execute_stored_entries_for_path_exact(
        &mut self,
        path: Vec<String>,
        extra: &T,
        keys: Option<&[serde_json::Value]>,
    ) -> Result<bool, error::DecSyncError> {
        let hash = hash::path_to_hash(path.clone());
        let file = self.own_v2_dir().join(&hash);
        let mut entries = read_entries(&file)
            .into_values()
            .filter(|entry| entry.path == path)
            .filter(|entry| keys.is_none_or(|keys| keys.contains(&entry.entry.key)))
            .collect::<Vec<_>>();
        Ok(execute_entries(&mut self.listeners, &mut entries, extra))
    }

    pub fn execute_stored_entries_for_path_prefix(
        &mut self,
        prefix: Vec<String>,
        extra: &T,
        keys: Option<&[serde_json::Value]>,
    ) -> Result<bool, error::DecSyncError> {
        let mut all_success = true;
        for hash in hash::all_hashes() {
            let file = self.own_v2_dir().join(&hash);
            let mut entries = read_entries(&file)
                .into_values()
                .filter(|entry| entry.path.starts_with(&prefix))
                .filter(|entry| keys.is_none_or(|keys| keys.contains(&entry.entry.key)))
                .collect::<Vec<_>>();
            if !entries.is_empty() {
                all_success &= execute_entries(&mut self.listeners, &mut entries, extra);
            }
        }
        Ok(all_success)
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

fn group_by_path(
    entries_with_path: Vec<entry::EntryWithPath>,
) -> Vec<(Vec<String>, Vec<entry::Entry>)> {
    let mut groups: BTreeMap<Vec<String>, Vec<entry::Entry>> = BTreeMap::new();
    for entry_with_path in entries_with_path {
        groups
            .entry(entry_with_path.path)
            .or_default()
            .push(entry_with_path.entry);
    }
    groups.into_iter().collect()
}

fn update_entries<T>(
    listeners: Option<&mut [Listener<T>]>,
    file: &PathBuf,
    entries_with_path: Vec<entry::EntryWithPath>,
    require_new_value: bool,
    extra: Option<&T>,
) -> Result<UpdateOutcome, error::DecSyncError> {
    let stored_entries = read_entries(file);

    let mut new_entries = entries_with_path
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

    let success = match (listeners, extra) {
        (Some(listeners), Some(extra_value)) => {
            execute_entries(listeners, &mut new_entries, extra_value)
        }
        _ => true,
    };

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

    Ok(UpdateOutcome {
        entries: new_entries,
        success,
    })
}

fn execute_entries<T>(
    listeners: &mut [Listener<T>],
    entries_with_path: &mut Vec<entry::EntryWithPath>,
    extra: &T,
) -> bool {
    let mut all_success = true;

    for (path, mut entries) in group_by_path(std::mem::take(entries_with_path)) {
        let success = call_listener(listeners, &path, &mut entries, extra);
        all_success &= success;
        entries_with_path.extend(entries.into_iter().map(|entry| entry::EntryWithPath {
            path: path.clone(),
            entry,
        }));
    }

    all_success
}

fn call_listener<T>(
    listeners: &mut [Listener<T>],
    path: &[String],
    entries: &mut Vec<entry::Entry>,
    extra: &T,
) -> bool {
    entries.retain(|entry| {
        !(path.len() == 1
            && path[0] == "info"
            && entry.key.as_str().is_some_and(|key| {
                key.starts_with("last-active-") || key.starts_with("supported-version-")
            }))
    });

    if entries.is_empty() {
        return true;
    }

    let Some(listener) = listeners
        .iter_mut()
        .find(|listener| path.starts_with(&listener.subpath))
    else {
        return true;
    };

    let relative_path = path[listener.subpath.len()..].to_vec();
    (listener.callback)(relative_path, entries, extra)
}

fn read_entries(file: &PathBuf) -> HashMap<(Vec<String>, serde_json::Value), entry::EntryWithPath> {
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

fn read_local_sequences(
    path: &PathBuf,
) -> Result<BTreeMap<String, BTreeMap<String, i64>>, error::DecSyncError> {
    Ok(fs::read_to_string(path)
        .ok()
        .and_then(|text| serde_json::from_str(&text).ok())
        .unwrap_or_default())
}

fn write_local_sequences(
    path: &PathBuf,
    sequences: &BTreeMap<String, BTreeMap<String, i64>>,
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

    fn instance(dir: &PathBuf) -> DecSyncInstance<()> {
        instance_with_app(dir, "app1")
    }

    fn instance_with_app(dir: &PathBuf, app_id: &str) -> DecSyncInstance<()> {
        DecSyncInstance::<()>::new(
            dir.to_string_lossy().into_owned(),
            "rss".to_owned(),
            None,
            app_id.to_owned(),
        )
    }

    fn read_local_sequences_json(dir: &PathBuf, app_id: &str) -> serde_json::Value {
        serde_json::from_str(
            &fs::read_to_string(dir.join("rss").join("local").join(app_id).join("sequences"))
                .unwrap(),
        )
        .unwrap()
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

    #[test]
    fn execute_all_new_entries_without_other_apps_is_noop() {
        let dir = test_dir("noop");
        let mut app1 = instance(&dir);

        app1.execute_all_new_entries(&()).unwrap();

        assert!(!app1.local_dir().join("sequences").exists());
        assert!(!app1.own_v2_dir().join("sequences").exists());
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn execute_all_new_entries_merges_other_apps_entries() {
        let dir = test_dir("pull");
        let app1 = instance_with_app(&dir, "app1");
        let mut app2 = instance_with_app(&dir, "app2");

        app1.set_entry(
            vec!["feeds".to_owned(), "subscriptions".to_owned()],
            serde_json::json!("https://foo.example.com/rss"),
            serde_json::json!(true),
        )
        .unwrap();

        app2.execute_all_new_entries(&()).unwrap();

        let content = fs::read_to_string(app2.own_v2_dir().join("b9")).unwrap();
        let lines = content.lines().collect::<Vec<_>>();
        assert_eq!(lines.len(), 1);
        let parsed: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(parsed[0], serde_json::json!(["feeds", "subscriptions"]));
        assert_eq!(parsed[2], serde_json::json!("https://foo.example.com/rss"));
        assert_eq!(parsed[3], serde_json::json!(true));

        assert_eq!(
            read_local_sequences_json(&dir, "app2"),
            serde_json::json!({"app1": {"b9": 1}})
        );
        assert!(!app2.own_v2_dir().join("sequences").exists());

        app2.execute_all_new_entries(&()).unwrap();

        assert_eq!(
            read_local_sequences_json(&dir, "app2"),
            serde_json::json!({"app1": {"b9": 1}})
        );
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn newer_entry_wins_across_apps() {
        let dir = test_dir("converge");
        let mut app1 = instance_with_app(&dir, "app1");
        let mut app2 = instance_with_app(&dir, "app2");
        let path = vec!["feeds".to_owned(), "subscriptions".to_owned()];

        app1.set_entries(vec![entry::EntryWithPath::new(
            path.clone(),
            "2020-07-17T12:30:00".to_owned(),
            serde_json::json!("url"),
            serde_json::json!(true),
        )])
        .unwrap();
        app2.set_entries(vec![entry::EntryWithPath::new(
            path,
            "2020-07-17T12:40:00".to_owned(),
            serde_json::json!("url"),
            serde_json::json!(false),
        )])
        .unwrap();

        app1.execute_all_new_entries(&()).unwrap();
        app2.execute_all_new_entries(&()).unwrap();

        for instance in [&app1, &app2] {
            let content = fs::read_to_string(instance.own_v2_dir().join("b9")).unwrap();
            let lines = content.lines().collect::<Vec<_>>();
            assert_eq!(lines.len(), 1);
            let parsed: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
            assert_eq!(parsed[3], serde_json::json!(false));
        }

        assert_eq!(
            read_local_sequences_json(&dir, "app1"),
            serde_json::json!({"app2": {"b9": 1}})
        );
        assert_eq!(
            read_local_sequences_json(&dir, "app2"),
            serde_json::json!({"app1": {"b9": 1}})
        );
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn local_sequences_gate_which_entries_are_merged() {
        let dir = test_dir("gating");
        let app1 = instance_with_app(&dir, "app1");
        let mut app2 = instance_with_app(&dir, "app2");
        let path = vec!["feeds".to_owned(), "subscriptions".to_owned()];

        app1.set_entries(vec![
            entry::EntryWithPath::new(
                path.clone(),
                "2020-07-17T12:30:00".to_owned(),
                serde_json::json!("a"),
                serde_json::json!(true),
            ),
            entry::EntryWithPath::new(
                path.clone(),
                "2020-07-17T12:31:00".to_owned(),
                serde_json::json!("b"),
                serde_json::json!(true),
            ),
        ])
        .unwrap();

        app2.execute_all_new_entries(&()).unwrap();

        let content = fs::read_to_string(app2.own_v2_dir().join("b9")).unwrap();
        assert_eq!(content.lines().count(), 2);

        let remaining = content
            .lines()
            .filter(|line| line.contains("\"b\""))
            .collect::<Vec<_>>()
            .join("\n");
        fs::write(app2.own_v2_dir().join("b9"), remaining).unwrap();

        app2.execute_all_new_entries(&()).unwrap();

        let content = fs::read_to_string(app2.own_v2_dir().join("b9")).unwrap();
        assert_eq!(content.lines().count(), 1);

        app1.set_entry(path, serde_json::json!("c"), serde_json::json!(true))
            .unwrap();

        app2.execute_all_new_entries(&()).unwrap();

        let content = fs::read_to_string(app2.own_v2_dir().join("b9")).unwrap();
        let keys = content
            .lines()
            .map(|line| {
                let parsed: serde_json::Value = serde_json::from_str(line).unwrap();
                parsed[2].clone()
            })
            .collect::<Vec<_>>();
        assert!(keys.contains(&serde_json::json!("a")));
        assert!(keys.contains(&serde_json::json!("b")));
        assert!(keys.contains(&serde_json::json!("c")));

        assert_eq!(
            read_local_sequences_json(&dir, "app2"),
            serde_json::json!({"app1": {"b9": 2}})
        );
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn listener_receives_new_entries_with_relative_path_and_extra() {
        let dir = test_dir("listener");
        let app1 = instance(&dir);
        let mut app2 = DecSyncInstance::<i32>::new(
            dir.to_string_lossy().into_owned(),
            "rss".to_owned(),
            None,
            "app2".to_owned(),
        );

        let captured = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
        let captured_in_listener = captured.clone();
        app2.add_multi_listener(vec!["feeds".to_owned()], move |path, entries, extra| {
            captured_in_listener.borrow_mut().push((
                path,
                entries
                    .iter()
                    .map(|entry| entry.key.clone())
                    .collect::<Vec<_>>(),
                *extra,
            ));
        });

        app1.set_entries(vec![
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

        app2.execute_all_new_entries(&7).unwrap();

        let captured = captured.borrow();
        assert_eq!(captured.len(), 1);
        let (path, keys, extra) = &captured[0];
        assert_eq!(path, &vec!["subscriptions".to_owned()]);
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&serde_json::json!("a")));
        assert!(keys.contains(&serde_json::json!("b")));
        assert_eq!(*extra, 7);
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn failed_listener_entries_are_not_merged_or_acknowledged() {
        let dir = test_dir("fail");
        let app1 = instance(&dir);
        let mut app2 = instance_with_app(&dir, "app2");

        app1.set_entries(vec![
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

        let accepted = std::rc::Rc::new(std::cell::RefCell::new(vec!["b".to_owned()]));
        let accepted_in_listener = accepted.clone();
        app2.add_listener_with_success(vec!["feeds".to_owned()], move |_, entry, _| {
            accepted_in_listener
                .borrow()
                .contains(&entry.key.as_str().unwrap().to_owned())
        });

        app2.execute_all_new_entries(&()).unwrap();

        let content = fs::read_to_string(app2.own_v2_dir().join("b9")).unwrap();
        let keys = content
            .lines()
            .map(|line| {
                let parsed: serde_json::Value = serde_json::from_str(line).unwrap();
                parsed[2].clone()
            })
            .collect::<Vec<_>>();
        assert_eq!(keys, vec![serde_json::json!("b")]);
        assert!(!app2.local_dir().join("sequences").exists());

        *accepted.borrow_mut() = vec!["a".to_owned(), "b".to_owned()];
        app2.execute_all_new_entries(&()).unwrap();

        let content = fs::read_to_string(app2.own_v2_dir().join("b9")).unwrap();
        assert_eq!(content.lines().count(), 2);
        assert_eq!(
            read_local_sequences_json(&dir, "app2"),
            serde_json::json!({"app1": {"b9": 1}})
        );
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn failed_multi_listener_entries_are_not_merged() {
        let dir = test_dir("fail-multi");
        let app1 = instance(&dir);
        let mut app2 = instance_with_app(&dir, "app2");

        app1.set_entries(vec![entry::EntryWithPath::new(
            vec!["feeds".to_owned(), "subscriptions".to_owned()],
            "2020-07-17T12:30:00".to_owned(),
            serde_json::json!("a"),
            serde_json::json!(true),
        )])
        .unwrap();

        app2.add_multi_listener_with_success(vec!["feeds".to_owned()], |_, _, _| false);

        app2.execute_all_new_entries(&()).unwrap();

        assert!(!app2.own_v2_dir().join("b9").exists());
        assert!(!app2.local_dir().join("sequences").exists());
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn init_stored_entries_merges_without_listeners() {
        let dir = test_dir("init");
        let app1 = instance(&dir);
        let mut app2 = instance_with_app(&dir, "app2");

        app1.set_entries(vec![entry::EntryWithPath::new(
            vec!["feeds".to_owned(), "subscriptions".to_owned()],
            "2020-07-17T12:30:00".to_owned(),
            serde_json::json!("a"),
            serde_json::json!(true),
        )])
        .unwrap();

        let called = std::rc::Rc::new(std::cell::Cell::new(false));
        let called_in_listener = called.clone();
        app2.add_listener(vec!["feeds".to_owned()], move |_, _, _| {
            called_in_listener.set(true);
        });

        app2.init_stored_entries().unwrap();

        assert!(!called.get());
        let content = fs::read_to_string(app2.own_v2_dir().join("b9")).unwrap();
        assert_eq!(content.lines().count(), 1);
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn execute_stored_entry_retroactively_executes() {
        let dir = test_dir("stored");
        let app1 = instance(&dir);
        let mut app2 = instance_with_app(&dir, "app2");

        app1.set_entry(
            vec!["feeds".to_owned(), "names".to_owned()],
            serde_json::json!("https://foo.example.com/rss"),
            serde_json::json!("Foo"),
        )
        .unwrap();
        app2.init_stored_entries().unwrap();

        let executed = std::rc::Rc::new(std::cell::RefCell::new(None::<serde_json::Value>));
        let executed_in_listener = executed.clone();
        app2.add_listener(vec!["feeds".to_owned()], move |_, entry, _| {
            *executed_in_listener.borrow_mut() = Some(entry.value.clone());
        });

        app2.execute_stored_entry(
            vec!["feeds".to_owned(), "names".to_owned()],
            serde_json::json!("https://foo.example.com/rss"),
            &(),
        )
        .unwrap();

        let executed = executed.borrow();
        assert_eq!(*executed, Some(serde_json::json!("Foo")));
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn execute_stored_entries_batches_by_path() {
        let dir = test_dir("stored-batch");
        let app1 = instance(&dir);
        let mut app2 = instance_with_app(&dir, "app2");

        app1.set_entries(vec![
            entry::EntryWithPath::new(
                vec!["feeds".to_owned(), "names".to_owned()],
                "2020-07-17T12:30:00".to_owned(),
                serde_json::json!("u1"),
                serde_json::json!("Feed1"),
            ),
            entry::EntryWithPath::new(
                vec!["feeds".to_owned(), "names".to_owned()],
                "2020-07-17T12:31:00".to_owned(),
                serde_json::json!("u2"),
                serde_json::json!("Feed2"),
            ),
        ])
        .unwrap();
        app2.init_stored_entries().unwrap();

        let executed = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
        let executed_in_listener = executed.clone();
        app2.add_listener(vec![], move |path, entry, _| {
            executed_in_listener
                .borrow_mut()
                .push((path, entry.key.clone()));
        });

        app2.execute_stored_entries(
            vec![
                entry::StoredEntry::new(
                    vec!["feeds".to_owned(), "names".to_owned()],
                    serde_json::json!("u1"),
                ),
                entry::StoredEntry::new(
                    vec!["feeds".to_owned(), "names".to_owned()],
                    serde_json::json!("u2"),
                ),
            ],
            &(),
        )
        .unwrap();

        let executed = executed.borrow();
        assert_eq!(executed.len(), 2);
        assert!(executed
            .iter()
            .any(|(_, key)| *key == serde_json::json!("u1")));
        assert!(executed
            .iter()
            .any(|(_, key)| *key == serde_json::json!("u2")));
        assert!(executed
            .iter()
            .all(|(path, _)| *path == vec!["feeds".to_owned(), "names".to_owned()]));
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn execute_stored_entries_for_path_prefix_filters_and_limits_keys() {
        let dir = test_dir("prefix");
        let app1 = instance(&dir);
        let mut app2 = instance_with_app(&dir, "app2");

        app1.set_entries(vec![
            entry::EntryWithPath::new(
                vec!["feeds".to_owned(), "names".to_owned()],
                "2020-07-17T12:30:00".to_owned(),
                serde_json::json!("u1"),
                serde_json::json!("Feed1"),
            ),
            entry::EntryWithPath::new(
                vec!["feeds".to_owned(), "names".to_owned()],
                "2020-07-17T12:31:00".to_owned(),
                serde_json::json!("u2"),
                serde_json::json!("Feed2"),
            ),
            entry::EntryWithPath::new(
                vec!["categories".to_owned(), "names".to_owned()],
                "2020-07-17T12:32:00".to_owned(),
                serde_json::json!("c1"),
                serde_json::json!("Cat1"),
            ),
        ])
        .unwrap();
        app2.init_stored_entries().unwrap();

        let executed = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
        let executed_in_listener = executed.clone();
        app2.add_listener(vec![], move |path, entry, _| {
            executed_in_listener
                .borrow_mut()
                .push((path, entry.value.clone()));
        });

        app2.execute_stored_entries_for_path_prefix(
            vec!["feeds".to_owned()],
            &(),
            Some(&[serde_json::json!("u1")]),
        )
        .unwrap();

        let executed = executed.borrow();
        assert_eq!(executed.len(), 1);
        let (path, value) = &executed[0];
        assert_eq!(path, &vec!["feeds".to_owned(), "names".to_owned()]);
        assert_eq!(value, &serde_json::json!("Feed1"));
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn maintenance_info_entries_are_not_dispatched() {
        let dir = test_dir("info-filter");
        let app1 = instance(&dir);
        let mut app2 = instance_with_app(&dir, "app2");

        app1.set_entries(vec![
            entry::EntryWithPath::new(
                vec!["info".to_owned()],
                "2020-07-17T12:30:00".to_owned(),
                serde_json::json!("last-active-app1"),
                serde_json::json!("2020-07-17"),
            ),
            entry::EntryWithPath::new(
                vec!["info".to_owned()],
                "2020-07-17T12:31:00".to_owned(),
                serde_json::json!("name"),
                serde_json::json!("MyCollection"),
            ),
        ])
        .unwrap();
        app2.init_stored_entries().unwrap();

        let executed = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
        let executed_in_listener = executed.clone();
        app2.add_listener(vec![], move |_, entry, _| {
            executed_in_listener.borrow_mut().push(entry.key.clone());
        });

        app2.execute_stored_entries_for_path_exact(vec!["info".to_owned()], &(), None)
            .unwrap();

        let executed = executed.borrow();
        assert_eq!(executed.len(), 1);
        assert_eq!(executed[0], serde_json::json!("name"));
        let _ = fs::remove_dir_all(dir);
    }
}
