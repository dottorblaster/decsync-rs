use std::fs;
use std::path::PathBuf;

mod hash;

pub struct DecSyncInstance {
    pub decsync_dir: String,
    pub sync_type: String,
    pub collection: Option<String>,
    pub own_app_id: String,
}

impl DecSyncInstance {
    pub fn new(
        decsync_dir: String,
        sync_type: String,
        collection: Option<String>,
        own_app_id: String,
    ) -> DecSyncInstance {
        let decsync_path = get_decsync_subdir(&decsync_dir, &sync_type, &collection);
        let _ = fs::create_dir_all(decsync_path);

        DecSyncInstance {
            decsync_dir,
            sync_type,
            collection,
            own_app_id,
        }
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
            "directory".to_owned(),
            "all".to_owned(),
            Some("all".to_owned()),
            "org.gnome.Newsflash".to_owned(),
        );
        assert_eq!(
            decsync_instance.own_app_id,
            "org.gnome.Newsflash".to_owned()
        )
    }
}
