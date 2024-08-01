mod hash;

pub struct DecSyncInstance {
    pub decsync_dir: String,
    pub sync_type: String,
    pub collection: String,
    pub own_app_id: String,
}

impl DecSyncInstance {
    pub fn new(
        decsync_dir: String,
        sync_type: String,
        collection: String,
        own_app_id: String,
    ) -> DecSyncInstance {
        DecSyncInstance {
            decsync_dir,
            sync_type,
            collection,
            own_app_id,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decsync_is_constructed() {
        let decsync_instance = DecSyncInstance::new(
            "directory".to_owned(),
            "all".to_owned(),
            "all".to_owned(),
            "org.gnome.Newsflash".to_owned(),
        );
        assert_eq!(
            decsync_instance.own_app_id,
            "org.gnome.Newsflash".to_owned()
        )
    }
}
