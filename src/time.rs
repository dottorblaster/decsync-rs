use chrono::{DateTime, Utc};

pub fn current_date_time() -> String {
    let now = Utc::now();
    DateTime::to_rfc3339(&now)
}
