mod kv;
mod lease;

pub use kv::KvClientPrefix;
pub use lease::LeaseClientPrefix;

trait VecExt {
    fn strip_prefix(&mut self, prefix: &[u8]);
    fn prefix_with(&mut self, prefix: &[u8]);
    fn prefix_range_end_with(&mut self, prefix: &[u8]);
}

impl VecExt for Vec<u8> {
    fn strip_prefix(&mut self, prefix: &[u8]) {
        if prefix.is_empty() {
            return;
        }

        if !self.starts_with(prefix) {
            return;
        }

        let pfx_len = prefix.len();
        let key_len = self.len();
        let new_len = key_len - pfx_len;
        unsafe {
            let ptr = self.as_mut_ptr();
            std::ptr::copy(ptr.add(pfx_len), ptr, new_len);
            self.set_len(new_len);
        }
    }

    fn prefix_with(&mut self, prefix: &[u8]) {
        if prefix.is_empty() {
            return;
        }

        let pfx_len = prefix.len();
        let key_len = self.len();
        self.reserve(pfx_len);
        unsafe {
            let ptr = self.as_mut_ptr();
            std::ptr::copy(ptr, ptr.add(pfx_len), key_len);
            std::ptr::copy_nonoverlapping(prefix.as_ptr(), ptr, pfx_len);
            self.set_len(key_len + pfx_len);
        }
    }

    fn prefix_range_end_with(&mut self, prefix: &[u8]) {
        if prefix.is_empty() {
            return;
        }

        if self.len() == 1 && self[0] == 0 {
            // the edge of the keyspace
            self.clear();
            self.extend_from_slice(prefix);
            let mut ok = false;
            for i in (0..self.len()).rev() {
                self[i] = self[i].wrapping_add(1);
                if self[i] != 0 {
                    ok = true;
                    break;
                }
            }
            if !ok {
                // 0xff..ff => 0x00
                self.clear();
                self.push(0);
            }
        } else if !self.is_empty() {
            self.prefix_with(prefix);
        }
    }
}

fn prefix_internal(pfx: &[u8], mut key: Vec<u8>, mut end: Vec<u8>) -> (Vec<u8>, Vec<u8>) {
    key.prefix_with(pfx);
    end.prefix_range_end_with(pfx);
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
