const HASH_BINS: i32 = 256;

pub fn path_to_hash(path: Vec<String>) -> String {
    if path.len() == 1 && path[0] == "info" {
        return "info".to_owned();
    }

    let string_hashes = path
        .iter()
        .map(|chunk| {
            poly_hash(
                19,
                chunk.as_bytes().iter().map(|byte| *byte as i32).collect(),
            )
        })
        .collect();

    format!("{:0>2x}", poly_hash(199, string_hashes))
}

fn poly_hash(p: i32, xs: Vec<i32>) -> i32 {
    xs.into_iter().fold(0, |hash, x| (hash * p + x) % HASH_BINS)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_hashing() {
        let path = vec!["foo".to_owned(), "bar".to_owned(), "baz".to_owned()];
        assert_eq!(path_to_hash(path), "e2".to_owned());

        let path = vec!["feeds".to_owned(), "subscriptions".to_owned()];
        assert_eq!(path_to_hash(path), "b9".to_owned());

        let path = vec![
            "entries".to_owned(),
            "read".to_owned(),
            "2020".to_owned(),
            "09".to_owned(),
            "10".to_owned(),
        ];
        assert_eq!(path_to_hash(path), "b8".to_owned());

        let path = vec![
            "entries".to_owned(),
            "read".to_owned(),
            "2020".to_owned(),
            "08".to_owned(),
            "23".to_owned(),
        ];
        assert_eq!(path_to_hash(path), "07".to_owned());
    }

    #[test]
    fn info() {
        let path = vec!["info".to_owned()];
        assert_eq!(path_to_hash(path), "info".to_owned())
    }

    #[test]
    fn only_exact_info_path_is_exempt() {
        let path = vec!["feeds".to_owned(), "info".to_owned()];
        assert_ne!(path_to_hash(path), "info".to_owned());
    }
}
