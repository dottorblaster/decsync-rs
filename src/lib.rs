#![warn(missing_docs)]

//! Synchronize key-value entries through a shared directory.
//!
//! DecSync is a file layout and a set of merge rules that turn a
//! plain directory into a conflict-free key-value store shared by
//! several devices. An entry is a path (list of strings), a key, a
//! value and a write timestamp. When two devices write the same
//! path/key, the newest timestamp wins; everything else converges by
//! replaying what could not be applied yet. The directory is mirrored
//! between devices by whatever file sync tool you already run
//! (Syncthing, Dropbox, cron'd rsync).
//!
//! This crate implements the **v2** file format. v1 directories are
//! not understood: a version other than v2 in `.decsync-info` fails
//! with [`error::DecSyncError::UnsupportedVersion`].
//!
//! # Layout
//!
//! ```text
//! DECSYNC_DIR/
//! ├── .decsync-info              format version marker
//! └── <sync-type>/               e.g. "rss", "contacts"
//!     └── <collection-id>/       when a sync type has several collections
//!         ├── v2/<app-id>/       your entries, one JSON document per line
//!         │   ├── 00 .. ff       paths hashed into these 256 buckets
//!         │   ├── info           entries with the path ["info"]
//!         │   └── sequences      how often each bucket changed
//!         └── local/<app-id>/    bookkeeping for this device only
//!             ├── info           version and last active date
//!             └── sequences      what was already read from the others
//! ```
//!
//! Each application only writes inside its own `v2/<app-id>`
//! directory, so devices never compete for a file. New entries are
//! discovered by comparing `sequences`; pull them with
//! [`execute_all_new_entries`](DecSyncInstance::execute_all_new_entries).
//!
//! # The appId
//!
//! [`own_app_id`](DecSyncInstance::own_app_id) must be unique per
//! device and stable across restarts. Two instances running at the
//! same time with the same appId will fight over a directory;
//! reinstalling an app may reuse its old id.
//!
//! # Example
//!
//! ```
//! # use decsync::DecSyncInstance;
//! // One directory, mirrored to a second machine by file sync.
//! let dir = std::env::temp_dir().join("decsync-docs-root");
//! let _ = std::fs::remove_dir_all(&dir);
//!
//! let mut phone = DecSyncInstance::<u32>::new(
//!     dir.to_string_lossy().into_owned(),
//!     "rss".to_owned(),
//!     None,
//!     "phone".to_owned(),
//! )
//! .unwrap();
//! let mut laptop = DecSyncInstance::<u32>::new(
//!     dir.to_string_lossy().into_owned(),
//!     "rss".to_owned(),
//!     None,
//!     "laptop".to_owned(),
//! )
//! .unwrap();
//!
//! phone
//!     .set_entry(
//!         vec!["feeds".to_owned(), "subscriptions".to_owned()],
//!         serde_json::json!("https://example.com/feed.rss"),
//!         serde_json::json!(true),
//!     )
//!     .unwrap();
//!
//! let seen = std::rc::Rc::new(std::cell::RefCell::new(None));
//! let on_laptop = seen.clone();
//! laptop.add_listener(vec![], move |path, entry, _| {
//!     *on_laptop.borrow_mut() = Some((path, entry.value));
//! });
//!
//! laptop.execute_all_new_entries(&7).unwrap();
//!
//! let seen = seen.borrow();
//! let (path, value) = seen.as_ref().unwrap();
//! assert_eq!(*path, vec!["feeds".to_owned(), "subscriptions".to_owned()]);
//! assert_eq!(*value, serde_json::json!(true));
//!
//! let _ = std::fs::remove_dir_all(&dir);
//! ```
//!
//! # Limitations
//!
//! - v1 directories are rejected up front.
//! - No event-driven watching. The [`Notifier`] polls for changes;
//!   entries arrive when
//!   [`execute_all_new_entries`](DecSyncInstance::execute_all_new_entries)
//!   is called.
//! - A [`DecSyncInstance`] is neither [`Send`] nor [`Sync`]: listeners
//!   are closures and dispatch mutates them. Use it from one thread.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

pub mod entry;
pub mod error;
mod file_utils;
mod hash;
mod notifier;
mod time;

pub use notifier::Notifier;

const CURRENT_VERSION: i64 = 2;

/// One application's handle on a DecSync directory.
///
/// `T` is the type of the `extra` value handed to listeners when an
/// entry is executed. The extra is chosen where the execution happens,
/// so one listener can see different extras across calls.
///
/// An instance is neither [`Send`] nor [`Sync`] (listeners are kept as
/// closures and mutated during dispatch). Keep instance and directory
/// on one thread at a time; the file format itself tolerates
/// concurrent processes because they write disjoint files.
pub struct DecSyncInstance<T> {
    /// Directory holding the synchronized files, as given to [`new`](Self::new).
    pub decsync_dir: String,
    /// Kind of data being synced, e.g. `"rss"` or `"calendars"`.
    pub sync_type: String,
    /// Collection inside `sync_type`, when there is more than one.
    pub collection: Option<String>,
    /// Identifier of this application; its `v2/<app-id>` directory is
    /// the only part of the store it may write.
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
    /// Opens the DecSync directory for `sync_type`/`collection` and
    /// sets up this application's `v2` and `local` directories.
    ///
    /// Creating the directory also writes `.decsync-info` when missing,
    /// so a fresh path works without any setup. A version other than
    /// v2, or garbage in the marker, is an error: the handle refuses
    /// to read formats it does not implement instead of guessing.
    pub fn new(
        decsync_dir: String,
        sync_type: String,
        collection: Option<String>,
        own_app_id: String,
    ) -> Result<DecSyncInstance<T>, error::DecSyncError> {
        check_decsync_info(&decsync_dir)?;

        let decsync_dir_path = get_decsync_subdir(&decsync_dir, &sync_type, collection.as_deref());
        let _ = fs::create_dir_all(decsync_dir_path.join("v2").join(&own_app_id));
        let _ = fs::create_dir_all(decsync_dir_path.join("local").join(&own_app_id));

        let local_info_file = decsync_dir_path
            .join("local")
            .join(&own_app_id)
            .join("info");
        let mut local_info = read_local_info(&local_info_file)?;
        if !local_info.contains_key("version") {
            local_info.insert("version".to_owned(), serde_json::json!(CURRENT_VERSION));
            write_local_info(&local_info_file, &local_info)?;
        }

        Ok(DecSyncInstance {
            decsync_dir,
            sync_type,
            collection,
            own_app_id,
            decsync_dir_path,
            listeners: Vec::new(),
        })
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

    /// Registers a listener for entries whose path starts with `subpath`.
    ///
    /// The callback gets the entry with `subpath` removed from its
    /// path, plus the `extra` handed to the executing call. Only the
    /// first matching listener (registration order) runs for a path,
    /// and the internal `last-active-*` / `supported-version-*`
    /// bookkeeping under `["info"]` is filtered out before dispatch.
    ///
    /// # Example
    ///
    /// ```
    /// # use decsync::DecSyncInstance;
    /// # let dir = std::env::temp_dir().join("decsync-docs-listener");
    /// # let _ = std::fs::remove_dir_all(&dir);
    /// let mut decsync = DecSyncInstance::<()>::new(
    ///     dir.to_string_lossy().into_owned(),
    ///     "rss".to_owned(),
    ///     None,
    ///     "reader".to_owned(),
    /// )
    /// .unwrap();
    ///
    /// let seen = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
    /// let to_reader = seen.clone();
    /// decsync.add_listener(vec!["feeds".to_owned()], move |path, entry, _| {
    ///     to_reader.borrow_mut().push((path, entry.key));
    /// });
    ///
    /// decsync
    ///     .set_entry(
    ///         vec!["feeds".to_owned(), "subscriptions".to_owned()],
    ///         serde_json::json!("https://example.com/feed.rss"),
    ///         serde_json::json!(true),
    ///     )
    ///     .unwrap();
    ///
    /// // Own writes never dispatch; replay the stored entry instead.
    /// // The listener sees the path minus its registered subpath.
    /// decsync
    ///     .execute_stored_entries_for_path_exact(
    ///         vec!["feeds".to_owned(), "subscriptions".to_owned()],
    ///         &(),
    ///         None,
    ///     )
    ///     .unwrap();
    ///
    /// let seen = seen.borrow();
    /// assert_eq!(seen.len(), 1);
    /// assert_eq!(seen[0].0, vec!["subscriptions".to_owned()]);
    /// assert_eq!(seen[0].1, serde_json::json!("https://example.com/feed.rss"));
    /// # let _ = std::fs::remove_dir_all(&dir);
    /// ```
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

    /// Like [`add_listener`](Self::add_listener), with a chance to refuse.
    ///
    /// Returning `false` drops the entry: it is neither saved locally
    /// nor acknowledged in the read state, and the next
    /// [`execute_all_new_entries`](Self::execute_all_new_entries) tries
    /// again. This is how a name for a not-yet-known feed waits for the
    /// feed to appear, then fires.
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

    /// One callback per path instead of one per entry.
    ///
    /// The whole batch of entries for the matched path is delivered as
    /// a slice. Use this when the action is per-path rather than
    /// per-entry.
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

    /// Like [`add_multi_listener`](Self::add_multi_listener); a `false`
    /// return rejects the entire batch.
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

    /// Sets a single entry and stamps it with the current time.
    ///
    /// The entry lands in this application's bucket for the hashed
    /// path and its sequence number goes up by one.
    ///
    /// # Example
    ///
    /// ```
    /// # use decsync::DecSyncInstance;
    /// # let dir = std::env::temp_dir().join("decsync-docs-set-entry");
    /// # let _ = std::fs::remove_dir_all(&dir);
    /// let decsync = DecSyncInstance::<()>::new(
    ///     dir.to_string_lossy().into_owned(),
    ///     "rss".to_owned(),
    ///     None,
    ///     "reader".to_owned(),
    /// )
    /// .unwrap();
    ///
    /// decsync
    ///     .set_entry(
    ///         vec!["feeds".to_owned(), "subscriptions".to_owned()],
    ///         serde_json::json!("https://example.com/feed.rss"),
    ///         serde_json::json!(true),
    ///     )
    ///     .unwrap();
    /// # let _ = std::fs::remove_dir_all(&dir);
    /// ```
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

    /// Sets several entries in one call.
    ///
    /// Entries sharing a path hash share one bucket file and one
    /// sequence bump, so batching beats a loop of
    /// [`set_entry`](Self::set_entry) whenever paths repeat.
    ///
    /// Entries older than what is already stored for the same path and
    /// key are dropped, and re-writing an identical value changes
    /// nothing. The format makes replays idempotent: feeding an old
    /// dataset in here will not clobber newer data.
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

    /// Pulls in everything the other applications wrote since the last
    /// pull, merging it into this application's own files.
    ///
    /// Buckets whose sequence number moved are read whole and merged;
    /// already-up-to-date entries fall out of the merge by their
    /// timestamps. The own `sequences` file stays untouched — other
    /// apps pick the originals up themselves — while the local read
    /// state is advanced so the next pull skips those buckets.
    ///
    /// Entries rejected by a listener are not merged here; they are
    /// retried on the next call. [`init_stored_entries`](Self::init_stored_entries)
    /// does the same pull without dispatching listeners.
    ///
    /// # Example
    ///
    /// ```
    /// # use decsync::DecSyncInstance;
    /// # let dir = std::env::temp_dir().join("decsync-docs-pull");
    /// # let _ = std::fs::remove_dir_all(&dir);
    /// let a = DecSyncInstance::<()>::new(
    ///     dir.to_string_lossy().into_owned(),
    ///     "rss".to_owned(),
    ///     None,
    ///     "device-a".to_owned(),
    /// )
    /// .unwrap();
    /// let mut b = DecSyncInstance::<()>::new(
    ///     dir.to_string_lossy().into_owned(),
    ///     "rss".to_owned(),
    ///     None,
    ///     "device-b".to_owned(),
    /// )
    /// .unwrap();
    ///
    /// a.set_entry(
    ///     vec!["feeds".to_owned(), "subscriptions".to_owned()],
    ///     serde_json::json!("https://example.com/feed.rss"),
    ///     serde_json::json!(true),
    /// )
    /// .unwrap();
    ///
    /// b.execute_all_new_entries(&()).unwrap();
    ///
    /// // The entry now sits in b's own files, ready to be executed.
    /// // `b9` is the bucket for ["feeds", "subscriptions"].
    /// let content = std::fs::read_to_string(
    ///     dir.join("rss").join("v2").join("device-b").join("b9"),
    /// )
    /// .unwrap();
    /// assert!(content.contains("https://example.com/feed.rss"));
    /// # let _ = std::fs::remove_dir_all(&dir);
    /// ```
    pub fn execute_all_new_entries(&mut self, extra: &T) -> Result<(), error::DecSyncError> {
        self.execute_all_new_entries_internal(Some(extra))
    }

    /// The same pull as [`execute_all_new_entries`](Self::execute_all_new_entries),
    /// without running any listener.
    ///
    /// The usual first thing an app does after (re)install: get the
    /// stored entries local, then replay what matters with `execute_stored_*`.
    pub fn init_stored_entries(&mut self) -> Result<(), error::DecSyncError> {
        self.execute_all_new_entries_internal(None)
    }

    /// Whether any other application has entries this instance has not
    /// pulled yet.
    ///
    /// Compares every other `v2/<app-id>/sequences` file against the
    /// local read state; nothing is written. Cheap enough to poll.
    ///
    /// # Example
    ///
    /// ```
    /// # use decsync::DecSyncInstance;
    /// # let dir = std::env::temp_dir().join("decsync-docs-pending");
    /// # let _ = std::fs::remove_dir_all(&dir);
    /// let mut app = DecSyncInstance::<()>::new(
    ///     dir.to_string_lossy().into_owned(),
    ///     "rss".to_owned(),
    ///     None,
    ///     "reader".to_owned(),
    /// )
    /// .unwrap();
    ///
    /// if app.has_new_entries().unwrap() {
    ///     app.execute_all_new_entries(&()).unwrap();
    /// }
    /// # let _ = std::fs::remove_dir_all(&dir);
    /// ```
    pub fn has_new_entries(&self) -> Result<bool, error::DecSyncError> {
        Ok(!self.pending_app_ids()?.is_empty())
    }

    /// The appIds with un-pulled entries, empty when everything is up
    /// to date.
    ///
    /// This is the signal a watcher or poller should react to: once it
    /// stops changing after a pull, the read state has caught up.
    /// [`execute_all_new_entries`](Self::execute_all_new_entries) is
    /// still the thing that actually merges.
    pub fn pending_app_ids(&self) -> Result<Vec<String>, error::DecSyncError> {
        compute_pending_app_ids(&self.decsync_dir_path, &self.own_app_id)
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

        self.maintain()?;

        Ok(())
    }

    fn maintain(&self) -> Result<(), error::DecSyncError> {
        let info_file = self.local_dir().join("info");
        let mut local_info = read_local_info(&info_file)?;

        let current_date = time::current_date();
        let date_changed = local_info
            .get("last-active")
            .and_then(|value| value.as_str())
            .is_none_or(|last_active| current_date.as_str() > last_active);
        if date_changed {
            let date_value = serde_json::json!(current_date);
            local_info.insert("last-active".to_owned(), date_value.clone());
            write_local_info(&info_file, &local_info)?;
            self.set_entry(
                vec!["info".to_owned()],
                serde_json::Value::from(format!("last-active-{}", self.own_app_id)),
                date_value,
            )?;
        }

        Ok(())
    }

    /// Replays one already-stored entry through its listener.
    ///
    /// Reads the current value from this application's own files and
    /// dispatches it; nothing is written. Use this for things that
    /// could not be applied when they arrived, once their
    /// prerequisites exist. Returns whether every listener accepted
    /// the entry.
    pub fn execute_stored_entry(
        &mut self,
        path: Vec<String>,
        key: serde_json::Value,
        extra: &T,
    ) -> Result<bool, error::DecSyncError> {
        self.execute_stored_entries_for_path_exact(path, extra, Some(std::slice::from_ref(&key)))
    }

    /// Several replays at once, grouped by path so one bucket read
    /// serves an entire path.
    ///
    /// Same semantics as [`execute_stored_entry`](Self::execute_stored_entry).
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

    /// Replays stored entries with exactly `path`, optionally
    /// restricted to some `keys`.
    ///
    /// `None` for `keys` means whatever keys are stored.
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

    /// Replays stored entries whose path starts with `prefix`,
    /// optionally restricted to some `keys`.
    ///
    /// Unlike the exact form, this scans all 256 buckets plus the
    /// `info` file, so it is the slow way to reach deeply nested
    /// entries. Keep it for initial loads and retries.
    ///
    /// # Example
    ///
    /// ```
    /// # use decsync::DecSyncInstance;
    /// # let dir = std::env::temp_dir().join("decsync-docs-replay");
    /// # let _ = std::fs::remove_dir_all(&dir);
    /// let mut decsync = DecSyncInstance::<()>::new(
    ///     dir.to_string_lossy().into_owned(),
    ///     "rss".to_owned(),
    ///     None,
    ///     "reader".to_owned(),
    /// )
    /// .unwrap();
    ///
    /// // A feed name arrives while the feed is not subscribed yet; it
    /// // is stored and replayed once the subscription shows up.
    /// decsync
    ///     .set_entries(vec![decsync::entry::EntryWithPath::new(
    ///         vec!["feeds".to_owned(), "names".to_owned()],
    ///         "2020-07-17T12:00:00".to_owned(),
    ///         serde_json::json!("https://example.com/feed.rss"),
    ///         serde_json::json!("Example feed"),
    ///     )])
    ///     .unwrap();
    ///
    /// let replayed = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
    /// let to_replayed = replayed.clone();
    /// decsync.add_listener(vec![], move |_, entry, _| {
    ///     to_replayed.borrow_mut().push(entry.value.clone());
    /// });
    ///
    /// decsync
    ///     .execute_stored_entries_for_path_prefix(vec!["feeds".to_owned()], &(), None)
    ///     .unwrap();
    ///
    /// let replayed = replayed.borrow();
    /// assert_eq!(replayed.len(), 1);
    /// assert_eq!(replayed[0], serde_json::json!("Example feed"));
    /// # let _ = std::fs::remove_dir_all(&dir);
    /// ```
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

fn sequences_have_news(
    app_sequences: &BTreeMap<String, i64>,
    app_id: &str,
    local_sequences: &BTreeMap<String, BTreeMap<String, i64>>,
) -> bool {
    app_sequences.iter().any(|(hash, sequence)| {
        let already_read = local_sequences
            .get(app_id)
            .and_then(|hashes| hashes.get(hash))
            .copied()
            .unwrap_or(0);
        *sequence != already_read
    })
}

fn compute_pending_app_ids(
    decsync_dir_path: &Path,
    own_app_id: &str,
) -> Result<Vec<String>, error::DecSyncError> {
    let v2_dir = decsync_dir_path.join("v2");
    let local_sequences = read_local_sequences(
        &decsync_dir_path
            .join("local")
            .join(own_app_id)
            .join("sequences"),
    )?;

    let mut pending = Vec::new();
    for app_id in file_utils::list_directories(&v2_dir)? {
        if app_id == own_app_id {
            continue;
        }
        let app_sequences = read_sequences(&v2_dir.join(&app_id).join("sequences"))?;
        if sequences_have_news(&app_sequences, &app_id, &local_sequences) {
            pending.push(app_id);
        }
    }
    Ok(pending)
}

fn read_local_info(
    path: &PathBuf,
) -> Result<BTreeMap<String, serde_json::Value>, error::DecSyncError> {
    Ok(fs::read_to_string(path)
        .ok()
        .and_then(|text| serde_json::from_str(&text).ok())
        .unwrap_or_default())
}

fn write_local_info(
    path: &PathBuf,
    info: &BTreeMap<String, serde_json::Value>,
) -> Result<(), error::DecSyncError> {
    file_utils::write_lines(path, vec![serde_json::to_string(info)?], false)
}

fn get_decsync_subdir(decsync_dir: &str, sync_type: &str, collection: Option<&str>) -> PathBuf {
    let mut decsync_path = PathBuf::new();

    decsync_path.push(decsync_dir);
    decsync_path.push(sync_type);

    if let Some(collection_path) = collection {
        decsync_path.push(collection_path);
    }

    decsync_path
}

/// Checks `.decsync-info` in `decsync_dir`, creating it with the
/// current version when it does not exist.
///
/// Malformed content becomes [`InvalidInfo`]; a version this crate
/// does not implement becomes [`UnsupportedVersion`].
///
/// # Example
///
/// ```
/// use decsync::check_decsync_info;
/// let dir = std::env::temp_dir().join("decsync-docs-info");
/// let _ = std::fs::remove_dir_all(&dir);
///
/// check_decsync_info(&dir.to_string_lossy()).unwrap();
///
/// let marker = std::fs::read_to_string(dir.join(".decsync-info")).unwrap();
/// assert_eq!(marker.trim(), r#"{"version":2}"#);
/// # let _ = std::fs::remove_dir_all(&dir);
/// ```
///
/// [`InvalidInfo`]: crate::error::DecSyncError::InvalidInfo
/// [`UnsupportedVersion`]: crate::error::DecSyncError::UnsupportedVersion
pub fn check_decsync_info(decsync_dir: &str) -> Result<(), error::DecSyncError> {
    let info_file = PathBuf::from(decsync_dir).join(".decsync-info");
    let info = read_decsync_info_or_default(&info_file)?;
    check_version(&info)
}

fn read_decsync_info_or_default(
    path: &PathBuf,
) -> Result<BTreeMap<String, serde_json::Value>, error::DecSyncError> {
    match fs::read_to_string(path) {
        Ok(text) => serde_json::from_str(&text).map_err(|_| error::DecSyncError::InvalidInfo),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let _ = path.parent().map(fs::create_dir_all);
            let default =
                BTreeMap::from([("version".to_owned(), serde_json::json!(CURRENT_VERSION))]);
            write_local_info(path, &default)?;
            Ok(default)
        }
        Err(_) => Err(error::DecSyncError::InvalidInfo),
    }
}

fn check_version(info: &BTreeMap<String, serde_json::Value>) -> Result<(), error::DecSyncError> {
    match info.get("version").and_then(|version| version.as_i64()) {
        Some(version) if version == CURRENT_VERSION => Ok(()),
        Some(found) => Err(error::DecSyncError::UnsupportedVersion {
            supported: CURRENT_VERSION,
            found,
        }),
        None => Err(error::DecSyncError::InvalidInfo),
    }
}

/// Names the collections under `decsync_dir/<sync-type>`.
///
/// Nothing is created along the way. Empty for sync types without
/// collections, so this only means something for multi-collection
/// sync types.
pub fn list_decsync_collections(
    decsync_dir: &str,
    sync_type: &str,
) -> Result<Vec<String>, error::DecSyncError> {
    file_utils::list_directories(&get_decsync_subdir(decsync_dir, sync_type, None))
}

/// The newest stored entries under `["info"]`, merged across all
/// applications.
///
/// These are the collection facts needed before opening a handle:
/// `"name"`, `"color"`, `"deleted"`, plus the `last-active-<appId>`
/// dates written during maintenance. Absent keys are simply
/// missing from the map.
///
/// # Example
///
/// ```
/// # use decsync::{DecSyncInstance, get_static_info};
/// # let dir = std::env::temp_dir().join("decsync-docs-static");
/// # let _ = std::fs::remove_dir_all(&dir);
/// let decsync = DecSyncInstance::<()>::new(
///     dir.to_string_lossy().into_owned(),
///     "calendars".to_owned(),
///     Some("col1".to_owned()),
///     "app".to_owned(),
/// )
/// .unwrap();
///
/// decsync
///     .set_entries(vec![decsync::entry::EntryWithPath::new(
///         vec!["info".to_owned()],
///         "2020-07-17T12:30:00".to_owned(),
///         serde_json::json!("name"),
///         serde_json::json!("Holidays"),
///     )])
///     .unwrap();
///
/// let info = get_static_info(dir.to_string_lossy().as_ref(), "calendars", Some("col1"))
///     .unwrap();
/// assert_eq!(
///     info.get(&serde_json::json!("name")),
///     Some(&serde_json::json!("Holidays"))
/// );
/// # let _ = std::fs::remove_dir_all(&dir);
/// ```
pub fn get_static_info(
    decsync_dir: &str,
    sync_type: &str,
    collection: Option<&str>,
) -> Result<HashMap<serde_json::Value, serde_json::Value>, error::DecSyncError> {
    let dir = get_decsync_subdir(decsync_dir, sync_type, collection).join("v2");
    let hash = hash::path_to_hash(vec!["info".to_owned()]);

    let mut info: HashMap<serde_json::Value, serde_json::Value> = HashMap::new();
    let mut datetimes: HashMap<serde_json::Value, String> = HashMap::new();

    for app_id in file_utils::list_directories(&dir)? {
        for entry in read_entries(&dir.join(&app_id).join(&hash)).into_values() {
            if entry.path.len() != 1 || entry.path[0] != "info" {
                continue;
            }
            let key = entry.entry.key;
            let datetime = entry.entry.datetime;
            if datetimes
                .get(&key)
                .is_none_or(|old_datetime| datetime.as_str() > old_datetime.as_str())
            {
                info.insert(key.clone(), entry.entry.value);
                datetimes.insert(key, datetime);
            }
        }
    }

    Ok(info)
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
        .unwrap()
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
    fn execute_all_new_entries_applies_maintenance_without_other_apps() {
        let dir = test_dir("noop");
        let mut app1 = instance(&dir);

        app1.execute_all_new_entries(&()).unwrap();

        assert!(!app1.local_dir().join("sequences").exists());
        assert!(app1.local_dir().join("info").exists());

        let sequences: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(app1.own_v2_dir().join("sequences")).unwrap())
                .unwrap();
        assert_eq!(sequences, serde_json::json!({"info": 1}));

        let info_file_content = fs::read_to_string(app1.own_v2_dir().join("info")).unwrap();
        assert_eq!(info_file_content.lines().count(), 1);
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
        let own_sequences: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(app2.own_v2_dir().join("sequences")).unwrap())
                .unwrap();
        assert_eq!(own_sequences, serde_json::json!({"info": 1}));

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
            serde_json::json!({"app1": {"b9": 1, "info": 1}})
        );
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn check_decsync_info_creates_default_and_validates() {
        let dir = test_dir("check-info");
        let path = dir.to_string_lossy().into_owned();

        check_decsync_info(&path).unwrap();

        let decsync_info: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(dir.join(".decsync-info")).unwrap()).unwrap();
        assert_eq!(decsync_info, serde_json::json!({"version": 2}));

        fs::write(dir.join(".decsync-info"), "{\"version\": 3}").unwrap();
        assert!(matches!(
            check_decsync_info(&path),
            Err(error::DecSyncError::UnsupportedVersion { .. })
        ));

        fs::write(dir.join(".decsync-info"), "not json").unwrap();
        assert!(matches!(
            check_decsync_info(&path),
            Err(error::DecSyncError::InvalidInfo)
        ));
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn constructor_rejects_unsupported_version() {
        let dir = test_dir("bad-version");
        let _ = fs::create_dir_all(&dir);
        fs::write(dir.join(".decsync-info"), "{\"version\": 1}").unwrap();

        let result = DecSyncInstance::<()>::new(
            dir.to_string_lossy().into_owned(),
            "rss".to_owned(),
            None,
            "app1".to_owned(),
        );
        assert!(matches!(
            result,
            Err(error::DecSyncError::UnsupportedVersion { .. })
        ));
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn list_decsync_collections_lists_directories() {
        let dir = test_dir("collections");
        let path = dir.to_string_lossy().into_owned();
        DecSyncInstance::<()>::new(
            path.clone(),
            "contacts".to_owned(),
            Some("col1".to_owned()),
            "app1".to_owned(),
        )
        .unwrap();
        DecSyncInstance::<()>::new(
            path.clone(),
            "contacts".to_owned(),
            Some("col2".to_owned()),
            "app2".to_owned(),
        )
        .unwrap();

        let collections = list_decsync_collections(&path, "contacts").unwrap();
        assert_eq!(collections, vec!["col1".to_owned(), "col2".to_owned()]);

        assert!(list_decsync_collections(&path, "rss").unwrap().is_empty());
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn get_static_info_returns_newest_info_entries() {
        let dir = test_dir("static-info");
        let path = dir.to_string_lossy().into_owned();
        let app1 = DecSyncInstance::<()>::new(
            path.clone(),
            "calendars".to_owned(),
            Some("col1".to_owned()),
            "app1".to_owned(),
        )
        .unwrap();
        let app2 = DecSyncInstance::<()>::new(
            path.clone(),
            "calendars".to_owned(),
            Some("col1".to_owned()),
            "app2".to_owned(),
        )
        .unwrap();

        app1.set_entries(vec![
            entry::EntryWithPath::new(
                vec!["info".to_owned()],
                "2020-07-17T12:30:00".to_owned(),
                serde_json::json!("name"),
                serde_json::json!("Calendar A"),
            ),
            entry::EntryWithPath::new(
                vec!["info".to_owned()],
                "2020-07-17T12:31:00".to_owned(),
                serde_json::json!("deleted"),
                serde_json::json!(false),
            ),
        ])
        .unwrap();
        app2.set_entries(vec![entry::EntryWithPath::new(
            vec!["info".to_owned()],
            "2020-07-17T12:40:00".to_owned(),
            serde_json::json!("name"),
            serde_json::json!("Calendar B"),
        )])
        .unwrap();

        let info = get_static_info(&path, "calendars", Some("col1")).unwrap();
        assert_eq!(
            info.get(&serde_json::json!("name")),
            Some(&serde_json::json!("Calendar B"))
        );
        assert_eq!(
            info.get(&serde_json::json!("deleted")),
            Some(&serde_json::json!(false))
        );
        assert_eq!(info.len(), 2);
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn maintenance_writes_local_info_and_last_active_entry() {
        let dir = test_dir("maintenance");
        let mut app1 = instance(&dir);
        let mut app2 = instance_with_app(&dir, "app2");

        app1.execute_all_new_entries(&()).unwrap();
        app2.execute_all_new_entries(&()).unwrap();

        let local_info: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(app1.local_dir().join("info")).unwrap())
                .unwrap();
        assert_eq!(local_info["version"], serde_json::json!(2));
        assert_eq!(local_info["last-active"].as_str().unwrap().len(), 10);

        let info = get_static_info(dir.to_string_lossy().as_ref(), "rss", None).unwrap();
        assert!(info.contains_key(&serde_json::json!("last-active-app1")));
        assert!(info.contains_key(&serde_json::json!("last-active-app2")));
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
        )
        .unwrap();

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

    #[test]
    fn pending_app_ids_reports_other_apps_with_news() {
        let dir = test_dir("pending");
        let app1 = instance(&dir);
        let mut app2 = instance_with_app(&dir, "app2");

        assert!(!app2.has_new_entries().unwrap());
        assert!(app2.pending_app_ids().unwrap().is_empty());

        app1.set_entry(
            vec!["feeds".to_owned(), "subscriptions".to_owned()],
            serde_json::json!("https://example.com/feed.rss"),
            serde_json::json!(true),
        )
        .unwrap();

        assert!(app2.has_new_entries().unwrap());
        assert_eq!(app2.pending_app_ids().unwrap(), vec!["app1".to_owned()]);

        app2.execute_all_new_entries(&()).unwrap();

        assert!(!app2.has_new_entries().unwrap());
        assert!(app2.pending_app_ids().unwrap().is_empty());
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn pending_detects_incremental_writes() {
        let dir = test_dir("pending-incremental");
        let app1 = instance(&dir);
        let mut app2 = instance_with_app(&dir, "app2");

        app1.set_entry(
            vec!["feeds".to_owned(), "subscriptions".to_owned()],
            serde_json::json!("https://example.com/feed.rss"),
            serde_json::json!(true),
        )
        .unwrap();
        app2.execute_all_new_entries(&()).unwrap();
        assert!(app2.pending_app_ids().unwrap().is_empty());

        app1.set_entry(
            vec!["feeds".to_owned(), "names".to_owned()],
            serde_json::json!("https://example.com/feed.rss"),
            serde_json::json!("Example"),
        )
        .unwrap();

        assert!(app2.has_new_entries().unwrap());
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn pending_ignores_own_activity() {
        let dir = test_dir("pending-own");
        let app1 = instance(&dir);
        let mut app2 = instance_with_app(&dir, "app2");

        app2.set_entry(
            vec!["feeds".to_owned(), "subscriptions".to_owned()],
            serde_json::json!("https://example.com/feed.rss"),
            serde_json::json!(true),
        )
        .unwrap();

        assert!(app2.pending_app_ids().unwrap().is_empty());

        app1.set_entry(
            vec!["feeds".to_owned(), "subscriptions".to_owned()],
            serde_json::json!("https://example.com/other.rss"),
            serde_json::json!(true),
        )
        .unwrap();

        assert_eq!(app2.pending_app_ids().unwrap(), vec!["app1".to_owned()]);
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn notifier_matches_instance_pending() {
        let dir = test_dir("notifier-pending");
        let app1 = instance(&dir);
        let app2 = instance_with_app(&dir, "app2");

        app1.set_entry(
            vec!["feeds".to_owned(), "subscriptions".to_owned()],
            serde_json::json!("https://example.com/feed.rss"),
            serde_json::json!(true),
        )
        .unwrap();

        let notifier = Notifier::new(&dir.to_string_lossy(), "rss", None, "app2");
        assert_eq!(
            notifier.pending_app_ids().unwrap(),
            app2.pending_app_ids().unwrap()
        );
        assert_eq!(notifier.pending_app_ids().unwrap(), vec!["app1".to_owned()]);
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn notifier_watch_delivers_pending_and_goes_quiet() {
        let dir = test_dir("notifier-watch");
        let app1 = instance(&dir);
        let mut app2 = instance_with_app(&dir, "app2");
        let interval = std::time::Duration::from_millis(10);

        app1.set_entry(
            vec!["feeds".to_owned(), "subscriptions".to_owned()],
            serde_json::json!("https://example.com/feed.rss"),
            serde_json::json!(true),
        )
        .unwrap();

        let (watcher, news) =
            Notifier::new(&dir.to_string_lossy(), "rss", None, "app2").watch(interval);

        let pending = news
            .recv_timeout(std::time::Duration::from_secs(2))
            .unwrap();
        assert_eq!(pending, vec!["app1".to_owned()]);

        app2.execute_all_new_entries(&()).unwrap();
        std::thread::sleep(interval * 5);

        let mut dropped = Vec::new();
        loop {
            match news.try_recv() {
                Ok(pending) => dropped.push(pending),
                _ => break,
            }
        }
        assert!(dropped.is_empty());

        drop(news);
        watcher.join();
        let _ = fs::remove_dir_all(dir);
    }
}
