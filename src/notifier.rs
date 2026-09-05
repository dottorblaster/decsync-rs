use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{sync_channel, Receiver};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use crate::error::DecSyncError;
use crate::{compute_pending_app_ids, get_decsync_subdir};

/// Polls a DecSync directory for un-pulled entries without holding an
/// instance.
///
/// A [`DecSyncInstance`](crate::DecSyncInstance) owns listener state and
/// is not [`Send`], so it cannot just move to a background thread.
/// This type only keeps the paths, which makes it safe to poll from
/// anywhere. It reports *that* something arrived; the merge still
/// happens on the instance, via
/// [`execute_all_new_entries`](crate::DecSyncInstance::execute_all_new_entries).
pub struct Notifier {
    decsync_dir_path: PathBuf,
    own_app_id: String,
}

impl Notifier {
    /// Points the poller at `sync_type`/`collection` in `decsync_dir`,
    /// tracking the read state of `own_app_id`. Nothing touches the
    /// disk yet.
    pub fn new(
        decsync_dir: &str,
        sync_type: &str,
        collection: Option<&str>,
        own_app_id: &str,
    ) -> Notifier {
        Notifier {
            decsync_dir_path: get_decsync_subdir(decsync_dir, sync_type, collection),
            own_app_id: own_app_id.to_owned(),
        }
    }

    /// The appIds that currently have un-pulled entries.
    ///
    /// Same signal as [`pending_app_ids`](crate::DecSyncInstance::pending_app_ids)
    /// on the instance, without requiring one.
    pub fn pending_app_ids(&self) -> Result<Vec<String>, DecSyncError> {
        compute_pending_app_ids(&self.decsync_dir_path, &self.own_app_id)
    }

    /// Spawns a thread that checks for pending entries every
    /// `interval` and pushes the appIds with news over the returned
    /// channel.
    ///
    /// Only non-empty results are pushed, never more than one at a
    /// time: a slow consumer skips stale notifications instead of
    /// building a backlog. Once a pull catches up, nothing is pushed
    /// until new entries arrive. Dropping the [`Watcher`] stops the
    /// thread after its current tick.
    ///
    /// # Example
    ///
    /// ```
    /// # use decsync::{DecSyncInstance, Notifier};
    /// # let dir = std::env::temp_dir().join("decsync-docs-notifier");
    /// # let _ = std::fs::remove_dir_all(&dir);
    /// // Another device wrote to the directory while we were away.
    /// let phone = DecSyncInstance::<()>::new(
    ///     dir.to_string_lossy().into_owned(),
    ///     "rss".to_owned(),
    ///     None,
    ///     "phone".to_owned(),
    /// )
    /// .unwrap();
    /// phone
    ///     .set_entry(
    ///         vec!["feeds".to_owned(), "subscriptions".to_owned()],
    ///         serde_json::json!("https://example.com/feed.rss"),
    ///         serde_json::json!(true),
    ///     )
    ///     .unwrap();
    ///
    /// let mut laptop = DecSyncInstance::<()>::new(
    ///     dir.to_string_lossy().into_owned(),
    ///     "rss".to_owned(),
    ///     None,
    ///     "laptop".to_owned(),
    /// )
    /// .unwrap();
    ///
    /// let (_watcher, news) = Notifier::new(&dir.to_string_lossy(), "rss", None, "laptop")
    ///     .watch(std::time::Duration::from_millis(50));
    ///
    /// let pending = news
    ///     .recv_timeout(std::time::Duration::from_millis(500))
    ///     .unwrap();
    /// assert_eq!(pending, ["phone".to_owned()]);
    ///
    /// laptop.execute_all_new_entries(&()).unwrap();
    /// # let _ = std::fs::remove_dir_all(&dir);
    /// ```
    pub fn watch(self, interval: Duration) -> (Watcher, Receiver<Vec<String>>) {
        let stop = Arc::new(AtomicBool::new(false));
        let (sender, receiver) = sync_channel(1);
        let handle = {
            let stop = stop.clone();
            thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    if let Ok(pending) =
                        compute_pending_app_ids(&self.decsync_dir_path, &self.own_app_id)
                    {
                        if !pending.is_empty() {
                            let _ = sender.try_send(pending);
                        }
                    }
                    thread::sleep(interval);
                }
            })
        };
        (
            Watcher {
                stop,
                handle: Some(handle),
            },
            receiver,
        )
    }
}

/// Stops a polling thread from [`Notifier::watch`].
///
/// Dropping or [`join`](Self::join)ing this handle asks the thread to
/// finish its current tick and exit.
pub struct Watcher {
    stop: Arc<AtomicBool>,
    handle: Option<thread::JoinHandle<()>>,
}

impl Watcher {
    /// Asks the thread to stop after its current tick.
    pub fn stop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
    }

    /// Stops the thread and waits for it to exit.
    pub fn join(mut self) {
        self.stop();
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for Watcher {
    fn drop(&mut self) {
        self.stop();
    }
}
