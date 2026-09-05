# decsync

Rust implementation of [DecSync](https://github.com/39aldo39/DecSync):
a set of key-value entries kept in sync between devices using nothing
but a shared directory. The directory is mirrored by whatever file
sync tool you already run; this crate only reads and writes files
inside it. For each path plus key, the newest timestamp wins.

The v2 file format is implemented. v1 directories are rejected, not
upgraded.

## Usage

```rust
use decsync::DecSyncInstance;

let mut decsync = DecSyncInstance::<()>::new(
    "/path/to/synced/dir".to_owned(),
    "rss".to_owned(),          // sync type
    None,                      // collection, only when a sync type has several
    "my-laptop".to_owned(),    // unique per device, stable across restarts
)
.unwrap();

decsync.set_entry(
    vec!["feeds".to_owned(), "subscriptions".to_owned()],
    serde_json::json!("https://example.com/feed.rss"),
    serde_json::json!(true),
)
.unwrap();

decsync.add_listener(vec!["feeds".to_owned()], |path, entry, _| {
    println!("feed {path:?} -> {entry:?}");
});

// everything other devices wrote since the last pull
decsync.execute_all_new_entries(&()).unwrap();
```

Listeners get the entry path with their registered subpath removed.
Entries that cannot be applied yet (a feed name before the feed
exists, say) are stored anyway and replayed later with
`execute_stored_entry` and friends.

## Limits

- No v1 support.
- No file watching: entries arrive when `execute_all_new_entries()`
  is called, not when the directory changes.
- An instance is not safe to share across threads; keep one per
  thread or process.

## License

Apache-2.0, see [LICENSE](LICENSE).