use std::collections::HashMap;
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

#[derive(PartialEq)]
enum NewValue {
    RequireNewValue,
    NoNewValue,
}

impl DecSyncInstance {
    pub fn new(
        decsync_dir: String,
        sync_type: String,
        collection: Option<String>,
        own_app_id: String,
    ) -> DecSyncInstance {
        let decsync_path = get_decsync_subdir(&decsync_dir, &sync_type, &collection);
        let _ = fs::create_dir_all(&decsync_path);

        DecSyncInstance {
            decsync_dir,
            sync_type,
            collection,
            own_app_id,
            decsync_dir_path: decsync_path,
        }
    }

    pub fn set_entries(
        &self,
        entries_with_path: Vec<entry::EntryWithPath>,
    ) -> Result<(), error::DecSyncError> {
        let grouped_entries: Vec<entry::GroupedEntries> = Vec::new();
        let sequences_file = self.decsync_dir_path.join("sequences");

        let mut sequences = Self::get_sequences(&sequences_file);

        let grouped_entries =
            entries_with_path
                .iter()
                .fold(grouped_entries, |mut accumulator, entry_with_path| {
                    let path = entry_with_path.path.clone();
                    let current_hash = hash::path_to_hash(path);

                    let mut updated = false;
                    accumulator.iter_mut().for_each(|grouped_entry| {
                        if current_hash == grouped_entry.hash {
                            let entry_to_push = entry::EntryWithPath {
                                path: entry_with_path.path.clone(),
                                entry: entry_with_path.entry.clone(),
                            };
                            grouped_entry.entries.push(entry_to_push);
                            updated = true;
                        }
                    });

                    if !updated {
                        let grouped_entry = entry::GroupedEntries {
                            hash: current_hash,
                            entries: vec![entry_with_path.clone()],
                        };
                        accumulator.push(grouped_entry)
                    }

                    accumulator
                });

        grouped_entries.iter().for_each(|group| {
            let path = self.decsync_dir_path.join(&group.hash);
            let extra: Option<bool> = None;
            let rest =
                Self::update_entries(path, group.entries.clone(), extra, NewValue::NoNewValue);

            match rest.unwrap_or_default().as_slice() {
                [] => {}
                [..] => {
                    sequences
                        .entry(group.hash.clone())
                        .and_modify(|value| *value += 1)
                        .or_insert(1);
                }
            }
        });

        Self::set_sequences(&sequences_file, sequences);

        Ok(())
    }

    fn update_entries<T>(
        file: PathBuf,
        entries_with_path: Vec<entry::EntryWithPath>,
        opt_extra: Option<T>,
        new_value: NewValue,
    ) -> Result<Vec<entry::EntryWithPath>, error::DecSyncError> {
        let mut stored_entries_with_path =
            HashMap::<(Vec<String>, serde_json::Value), entry::EntryWithPath>::new();

        fs::read_to_string(&file)
            .unwrap_or_default()
            .lines()
            .map(String::from)
            .filter_map(|line| entry::EntryWithPath::try_from(line).ok())
            .for_each(|entry_with_path| {
                stored_entries_with_path.insert(
                    (
                        entry_with_path.path.clone(),
                        entry_with_path.entry.key.clone(),
                    ),
                    entry_with_path,
                );
            });

        let newer_entries = entries_with_path
            .clone()
            .into_iter()
            .filter(|entry_with_path| {
                let stored_entry_with_path = stored_entries_with_path.get(&(
                    entry_with_path.path.clone(),
                    entry_with_path.entry.key.clone(),
                ));
                if let Some(stored_entry_with_path) = stored_entry_with_path {
                    if entry_with_path.entry.datetime <= stored_entry_with_path.entry.datetime
                        || (new_value == NewValue::RequireNewValue
                            && entry_with_path.entry.value == stored_entry_with_path.entry.value)
                    {
                        return false;
                    }
                }

                true
            })
            .collect();

        let update_result = match opt_extra {
            Some(extra_value) => Self::execute_entries(&entries_with_path, extra_value),
            None => Ok(()),
        };

        let mut stored_entries_removed = false;
        for entry_with_path in &entries_with_path {
            match stored_entries_with_path.remove(&(
                entry_with_path.path.clone(),
                entry_with_path.entry.key.clone(),
            )) {
                Some(_) => stored_entries_removed = true,
                None => {}
            }
        }

        let stored_entries_result = if stored_entries_removed {
            let stored_lines: Vec<String> = stored_entries_with_path
                .values()
                .map(|entry_with_path| entry_with_path.to_json().to_string())
                .collect();

            file_utils::write_lines(&file, stored_lines, false)
        } else {
            Ok(())
        };

        let saved_lines = entries_with_path
            .into_iter()
            .map(|entry_with_path| entry_with_path.to_json().to_string())
            .collect();
        let saved_lines_result = file_utils::write_lines(&file, saved_lines, true);

        for result in vec![update_result, stored_entries_result, saved_lines_result] {
            if let Err(error) = result {
                return Err(error);
            }
        }

        Ok(newer_entries)
    }

    fn execute_entries<T>(
        _entries_with_path: &Vec<entry::EntryWithPath>,
        _extra_value: T,
    ) -> Result<(), error::DecSyncError> {
        todo!()
    }

    fn get_sequences(_path: &PathBuf) -> HashMap<String, i64> {
        todo!()
    }

    fn set_sequences(_path: &PathBuf, _sequences: HashMap<String, i64>) {
        todo!()
    }
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

    #[test]
    fn decsync_is_constructed() {
        let decsync_instance = DecSyncInstance::new(
            "test/directory1".to_owned(),
            "all".to_owned(),
            Some("all".to_owned()),
            "org.gnome.Newsflash".to_owned(),
        );
        assert_eq!(
            decsync_instance.own_app_id,
            "org.gnome.Newsflash".to_owned()
        );

        let _ = fs::remove_dir_all("test/directory1");
    }
}
