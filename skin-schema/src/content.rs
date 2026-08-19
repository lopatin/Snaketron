//! Naming a skin document by its bytes.
//!
//! A player-authored skin travels as `sha256:<hex>` rather than as a catalogue
//! id, and that reference is what a spectator resolves, what a snapshot stores,
//! and what a replay renders years later. So it has to mean exactly one
//! document forever: an author editing their skin must not be able to change
//! what a finished game looked like.
//!
//! That only holds if two independent serializers agree on the bytes. They do
//! not agree by default — map ordering and whitespace are both free choices —
//! so this module defines one canonical encoding and hashes that. Everything
//! here is deliberately dependency-free and identical natively and in wasm,
//! because the server computes the reference on write and the client verifies
//! it on read, and a disagreement between the two would look like corruption.

use crate::SkinDoc;
use serde_json::{Map, Value};

/// The prefix every content-addressed reference carries.
pub const CONTENT_REF_PREFIX: &str = "sha256:";

/// Length of a full reference: the prefix plus 64 hex characters.
pub const CONTENT_REF_LENGTH: usize = CONTENT_REF_PREFIX.len() + 64;

/// The canonical bytes of a document.
///
/// Object keys sorted, no insignificant whitespace, numbers in serde_json's
/// own shortest round-trip form. Sorting is the load-bearing part: serde
/// preserves struct field order, but a document that has been through a map —
/// parsed from a request body, read back out of storage — carries whatever
/// order it arrived in, and two orders would hash differently while meaning
/// the same skin.
pub fn canonical_bytes(doc: &SkinDoc) -> Result<Vec<u8>, serde_json::Error> {
    let value = serde_json::to_value(doc)?;
    let mut out = Vec::new();
    write_canonical(&sorted(value), &mut out);
    Ok(out)
}

/// The reference a document is known by: `sha256:` plus 64 lowercase hex.
pub fn content_ref(doc: &SkinDoc) -> Result<String, serde_json::Error> {
    Ok(reference_for_bytes(&canonical_bytes(doc)?))
}

/// The reference for bytes that are already canonical.
pub fn reference_for_bytes(bytes: &[u8]) -> String {
    let mut reference = String::with_capacity(CONTENT_REF_LENGTH);
    reference.push_str(CONTENT_REF_PREFIX);
    for byte in sha256(bytes) {
        reference.push(hex_digit(byte >> 4));
        reference.push(hex_digit(byte & 0x0f));
    }
    reference
}

/// Whether a string is shaped like a content reference.
///
/// Shape only — this says nothing about whether the document exists, which is
/// storage's question, not this module's.
pub fn is_content_ref(reference: &str) -> bool {
    reference.len() == CONTENT_REF_LENGTH
        && reference.starts_with(CONTENT_REF_PREFIX)
        && reference[CONTENT_REF_PREFIX.len()..]
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn hex_digit(nibble: u8) -> char {
    char::from_digit(u32::from(nibble), 16).expect("a nibble is always a hex digit")
}

/// Recursively sort every object's keys.
fn sorted(value: Value) -> Value {
    match value {
        Value::Object(map) => {
            // BTreeMap ordering via sort: serde_json's Map may or may not
            // preserve insertion order depending on features, so the sort is
            // applied explicitly rather than assumed.
            let mut entries: Vec<(String, Value)> = map.into_iter().collect();
            entries.sort_by(|left, right| left.0.cmp(&right.0));
            let mut out = Map::with_capacity(entries.len());
            for (key, nested) in entries {
                out.insert(key, sorted(nested));
            }
            Value::Object(out)
        }
        Value::Array(items) => Value::Array(items.into_iter().map(sorted).collect()),
        scalar => scalar,
    }
}

/// Write a value with no insignificant whitespace.
fn write_canonical(value: &Value, out: &mut Vec<u8>) {
    match value {
        Value::Object(map) => {
            out.push(b'{');
            for (index, (key, nested)) in map.iter().enumerate() {
                if index > 0 {
                    out.push(b',');
                }
                write_canonical(&Value::String(key.clone()), out);
                out.push(b':');
                write_canonical(nested, out);
            }
            out.push(b'}');
        }
        Value::Array(items) => {
            out.push(b'[');
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    out.push(b',');
                }
                write_canonical(item, out);
            }
            out.push(b']');
        }
        scalar => out.extend_from_slice(scalar.to_string().as_bytes()),
    }
}

/// SHA-256.
///
/// Written out rather than taken as a dependency because this crate compiles to
/// wasm and is imported by both sides of the system; a hash is 60 lines and
/// pinning it here means the client and server can never disagree because of a
/// version skew in a third-party crate.
fn sha256(message: &[u8]) -> [u8; 32] {
    const K: [u32; 64] = [
        0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4,
        0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe,
        0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f,
        0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
        0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc,
        0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
        0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070, 0x19a4c116,
        0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
        0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7,
        0xc67178f2,
    ];

    let mut state: [u32; 8] = [
        0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab,
        0x5be0cd19,
    ];

    let mut padded = message.to_vec();
    let bit_length = (message.len() as u64) * 8;
    padded.push(0x80);
    while padded.len() % 64 != 56 {
        padded.push(0);
    }
    padded.extend_from_slice(&bit_length.to_be_bytes());

    for chunk in padded.chunks_exact(64) {
        let mut w = [0u32; 64];
        for (index, word) in chunk.chunks_exact(4).enumerate() {
            w[index] = u32::from_be_bytes([word[0], word[1], word[2], word[3]]);
        }
        for index in 16..64 {
            let s0 = w[index - 15].rotate_right(7)
                ^ w[index - 15].rotate_right(18)
                ^ (w[index - 15] >> 3);
            let s1 = w[index - 2].rotate_right(17)
                ^ w[index - 2].rotate_right(19)
                ^ (w[index - 2] >> 10);
            w[index] = w[index - 16]
                .wrapping_add(s0)
                .wrapping_add(w[index - 7])
                .wrapping_add(s1);
        }

        let [mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut h] = state;
        for index in 0..64 {
            let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let ch = (e & f) ^ ((!e) & g);
            let temp1 = h
                .wrapping_add(s1)
                .wrapping_add(ch)
                .wrapping_add(K[index])
                .wrapping_add(w[index]);
            let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let maj = (a & b) ^ (a & c) ^ (b & c);
            let temp2 = s0.wrapping_add(maj);

            h = g;
            g = f;
            f = e;
            e = d.wrapping_add(temp1);
            d = c;
            c = b;
            b = a;
            a = temp1.wrapping_add(temp2);
        }

        for (slot, value) in state.iter_mut().zip([a, b, c, d, e, f, g, h]) {
            *slot = slot.wrapping_add(value);
        }
    }

    let mut digest = [0u8; 32];
    for (index, word) in state.iter().enumerate() {
        digest[index * 4..index * 4 + 4].copy_from_slice(&word.to_be_bytes());
    }
    digest
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    /// The published vectors. If this hash is wrong, every reference in the
    /// system is wrong, and nothing downstream would notice — it would simply
    /// be internally consistent and incompatible with the rest of the world.
    #[test]
    fn sha256_matches_the_published_vectors() {
        assert_eq!(
            hex(&sha256(b"")),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(
            hex(&sha256(b"abc")),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        // Long enough to need a second block, which is where a padding bug hides.
        assert_eq!(
            hex(&sha256(
                b"abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq"
            )),
            "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1"
        );
    }

    #[test]
    fn a_reference_is_the_prefix_and_sixty_four_hex_digits() {
        let reference = reference_for_bytes(b"anything");
        assert_eq!(reference.len(), CONTENT_REF_LENGTH);
        assert!(reference.starts_with(CONTENT_REF_PREFIX));
        assert!(is_content_ref(&reference));
    }

    #[test]
    fn shape_checking_refuses_what_is_not_a_reference() {
        assert!(!is_content_ref("classic@1"));
        assert!(!is_content_ref("sha256:"));
        assert!(!is_content_ref(&format!(
            "{CONTENT_REF_PREFIX}{}",
            "g".repeat(64)
        )));
        assert!(
            !is_content_ref(&format!("{CONTENT_REF_PREFIX}{}", "A".repeat(64))),
            "hex is lowercase, so an uppercase twin cannot be a second name for one document"
        );
        assert!(!is_content_ref(&format!(
            "{CONTENT_REF_PREFIX}{}",
            "a".repeat(63)
        )));
    }

    /// The property the whole scheme rests on: key order must not change the
    /// reference, or the same skin would have two names depending on which
    /// serializer last touched it.
    #[test]
    fn key_order_does_not_change_the_canonical_bytes() {
        let first: Value = serde_json::json!({
            "b": 1,
            "a": {"z": [1, 2], "y": "text"},
        });
        let second: Value = serde_json::json!({
            "a": {"y": "text", "z": [1, 2]},
            "b": 1,
        });

        let mut left = Vec::new();
        write_canonical(&sorted(first), &mut left);
        let mut right = Vec::new();
        write_canonical(&sorted(second), &mut right);

        assert_eq!(left, right);
        assert_eq!(
            String::from_utf8(left).unwrap(),
            r#"{"a":{"y":"text","z":[1,2]},"b":1}"#
        );
    }

    /// Array order, unlike key order, is meaningful: layers paint in the order
    /// they are listed, so two orders are two different skins.
    #[test]
    fn array_order_does_change_the_canonical_bytes() {
        let mut left = Vec::new();
        write_canonical(&sorted(serde_json::json!([1, 2])), &mut left);
        let mut right = Vec::new();
        write_canonical(&sorted(serde_json::json!([2, 1])), &mut right);
        assert_ne!(left, right);
    }

    #[test]
    fn a_real_document_hashes_stably_and_differs_when_it_changes() {
        let mut doc: SkinDoc = serde_json::from_str(include_str!("../skins/aurora.skin.json"))
            .expect("the shipped aurora document parses");

        let reference = content_ref(&doc).expect("a valid document has a reference");
        assert!(is_content_ref(&reference));
        assert_eq!(
            reference,
            content_ref(&doc).expect("hashing is deterministic"),
            "the same document must hash the same way twice"
        );

        doc.name.push('!');
        assert_ne!(
            reference,
            content_ref(&doc).expect("a valid document has a reference"),
            "a changed document must get a different reference, or an edit \
             could silently rewrite what a finished game looked like"
        );
    }

    /// Round-tripping through a map is exactly what happens between a request
    /// body and storage, and it must not rename the document.
    #[test]
    fn a_document_survives_a_round_trip_through_untyped_json() {
        let doc: SkinDoc = serde_json::from_str(include_str!("../skins/lantern.skin.json"))
            .expect("the shipped lantern document parses");
        let direct = content_ref(&doc).expect("a valid document has a reference");

        let untyped: Value = serde_json::to_value(&doc).expect("serializes");
        let reparsed: SkinDoc = serde_json::from_value(untyped).expect("deserializes");
        assert_eq!(direct, content_ref(&reparsed).expect("still hashable"));
    }
}
