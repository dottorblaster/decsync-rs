const HASH_BINS: i32 = 256;

pub fn path_to_hash(path: Vec<String>) -> String {
    let info = "info".to_owned();

    if path.last() == Some(&info) {
        return info;
    }

    let string_hashes = path
        .iter()
        .map(|path_chunk| {
            let bytes = String::into_bytes(path_chunk.to_string())
                .iter()
                .map(|byte| str::parse::<i32>(&byte.to_string()).unwrap_or(0))
                .collect();

            poly_hash(19, bytes)
        })
        .collect();

    let hash = poly_hash(199, string_hashes);

    let result = format!("{hash:0>2x}");
    result
}

fn poly_hash(p: i32, xs: Vec<i32>) -> i32 {
    let mut hash = 0;

    for x in xs {
        hash *= p;
        hash += x;
        hash %= HASH_BINS;
    }

    hash
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
}
