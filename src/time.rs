use chrono::Utc;

pub fn current_date_time() -> String {
    Utc::now().format("%Y-%m-%dT%H:%M:%S").to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDateTime;

    #[test]
    fn datetime_is_fixed_width_utc() {
        let datetime = current_date_time();
        assert_eq!(datetime.len(), 19);
        assert!(NaiveDateTime::parse_from_str(&datetime, "%Y-%m-%dT%H:%M:%S").is_ok());
    }
}
