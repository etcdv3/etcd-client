mod lease;

fn prefix_with(pfx: &[u8], key: &mut Vec<u8>) {
    // HACK - this is effectively: key = pfx.append(key)
    let _ = key.splice(..0, pfx.to_vec()).collect::<Vec<_>>();
}

fn prefix_internal(pfx: &[u8], mut key: Vec<u8>, mut end: Vec<u8>) -> (Vec<u8>, Vec<u8>) {
    if pfx.is_empty() {
        return (key, end);
    }

    prefix_with(pfx, &mut key);

    if end.len() == 1 && end[0] == 0 {
        // the edge of the keyspace
        let mut new_end = pfx.to_vec();
        let mut ok = false;
        for i in (0..new_end.len()).rev() {
            new_end[i] = new_end[i].wrapping_add(1);
            if new_end[i] != 0 {
                ok = true;
                break;
            }
        }
        if !ok {
            // 0xff..ff => 0x00
            new_end = vec![0];
        }
        end = new_end;
    } else if end.len() >= 1 {
        prefix_with(pfx, &mut end);
    }

    (key, end)
}

#[cfg(test)]
mod tests {
    use crate::namespace::prefix_internal;

    #[test]
    fn test_prefix_internal() {
        fn run_test_case(pfx: &[u8], key: &[u8], end: &[u8], w_key: &[u8], w_end: &[u8]) {
            let (key, end) = prefix_internal(pfx, key.to_vec(), end.to_vec());
            assert_eq!(key, w_key);
            assert_eq!(end, w_end);
        }

        // single key
        run_test_case(b"pfx/", b"a", b"", b"pfx/a", b"");

        // range
        run_test_case(b"pfx/", b"abc", b"def", b"pfx/abc", b"pfx/def");

        // one-sided range (HACK - b'/' + 1 = b'0')
        run_test_case(b"pfx/", b"abc", b"\0", b"pfx/abc", b"pfx0");

        // one-sided range, end of keyspace
        run_test_case(b"\xFF\xFF", b"abc", b"\0", b"\xff\xffabc", b"\0");
    }
}
