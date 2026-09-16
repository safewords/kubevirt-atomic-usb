//! Handshake spoken on the pod network between the attaching agent (next to the VM) and the
//! exporting agent (next to the USB device), before the raw usbredir stream starts.
//!
//! ```text
//! attacher -> exporter: {"protocol":"atomic-usb/1","device":...,"claim":{...},"timestamp":...,"nonce":...,"mac":...}\n
//! exporter -> attacher: {"ok":true}\n        (or {"ok":false,"error":"..."}\n and close)
//! <usbredir protocol in both directions>
//! ```
//!
//! The MAC is HMAC-SHA256 over the request fields with a cluster-wide pre-shared key, which stops
//! arbitrary pods from grabbing USB devices. The stream itself is not encrypted; use a CNI with
//! transparent encryption (e.g. WireGuard) if the pod network is untrusted.

use std::collections::HashMap;

use hmac::{Hmac, KeyInit, Mac};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const PROTOCOL: &str = "atomic-usb/1";
/// Maximum accepted clock difference between nodes.
pub const MAX_CLOCK_SKEW_SECS: i64 = 120;
const MAX_LINE_BYTES: usize = 16 * 1024;

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ClaimRef {
    pub namespace: String,
    pub name: String,
    pub uid: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Hello {
    pub protocol: String,
    pub device: String,
    pub claim: ClaimRef,
    pub timestamp: i64,
    pub nonce: String,
    pub mac: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Reply {
    pub ok: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum AuthError {
    #[error("unsupported protocol {0:?}")]
    Protocol(String),
    #[error("invalid MAC")]
    Mac,
    #[error("timestamp outside the allowed clock skew")]
    Expired,
    #[error("nonce replayed")]
    Replay,
}

impl Hello {
    pub fn new(psk: &[u8], device: &str, claim: ClaimRef, timestamp: i64) -> Self {
        let mut hello = Hello {
            protocol: PROTOCOL.to_string(),
            device: device.to_string(),
            claim,
            timestamp,
            nonce: hex::encode(rand::random::<[u8; 16]>()),
            mac: String::new(),
        };
        hello.mac = hex::encode(hello.mac_for(psk).finalize().into_bytes());
        hello
    }

    fn mac_for(&self, psk: &[u8]) -> Hmac<Sha256> {
        let mut mac = <Hmac<Sha256> as KeyInit>::new_from_slice(psk).expect("HMAC accepts keys of any length");
        for field in [
            self.protocol.as_str(),
            self.device.as_str(),
            self.claim.namespace.as_str(),
            self.claim.name.as_str(),
            self.claim.uid.as_str(),
            &self.timestamp.to_string(),
            self.nonce.as_str(),
        ] {
            mac.update(&(field.len() as u64).to_be_bytes());
            mac.update(field.as_bytes());
        }
        mac
    }

    /// Verifies protocol, MAC and freshness. Replay protection is handled by [`NonceCache`].
    pub fn verify(&self, psk: &[u8], now: i64) -> Result<(), AuthError> {
        if self.protocol != PROTOCOL {
            return Err(AuthError::Protocol(self.protocol.clone()));
        }
        let expected = hex::decode(&self.mac).map_err(|_| AuthError::Mac)?;
        self.mac_for(psk).verify_slice(&expected).map_err(|_| AuthError::Mac)?;
        if (now - self.timestamp).abs() > MAX_CLOCK_SKEW_SECS {
            return Err(AuthError::Expired);
        }
        Ok(())
    }
}

/// Remembers nonces for the skew window so a captured handshake cannot be replayed.
#[derive(Default)]
pub struct NonceCache {
    seen: HashMap<String, i64>,
}

impl NonceCache {
    pub fn check_and_insert(&mut self, nonce: &str, timestamp: i64, now: i64) -> Result<(), AuthError> {
        self.seen.retain(|_, ts| (now - *ts).abs() <= MAX_CLOCK_SKEW_SECS);
        if self.seen.contains_key(nonce) {
            return Err(AuthError::Replay);
        }
        self.seen.insert(nonce.to_string(), timestamp);
        Ok(())
    }
}

/// Writes one JSON line.
pub async fn write_line<W: AsyncWrite + Unpin, T: Serialize>(w: &mut W, value: &T) -> std::io::Result<()> {
    let mut line = serde_json::to_vec(value).map_err(std::io::Error::other)?;
    line.push(b'\n');
    w.write_all(&line).await?;
    w.flush().await
}

/// Reads one JSON line byte by byte, so that no bytes of the following usbredir stream are consumed.
pub async fn read_line<R: AsyncRead + Unpin, T: DeserializeOwned>(r: &mut R) -> std::io::Result<T> {
    let mut line = Vec::with_capacity(256);
    loop {
        let byte = r.read_u8().await?;
        if byte == b'\n' {
            break;
        }
        if line.len() >= MAX_LINE_BYTES {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "handshake line too long",
            ));
        }
        line.push(byte);
    }
    serde_json::from_slice(&line).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn claim() -> ClaimRef {
        ClaimRef {
            namespace: "default".into(),
            name: "usb-claim".into(),
            uid: "4f0c".into(),
        }
    }

    #[test]
    fn valid_hello_verifies() {
        let hello = Hello::new(b"secret", "usb-1a86-7523-usb-serial-1234abcd", claim(), 1_000);
        assert_eq!(hello.verify(b"secret", 1_010), Ok(()));
    }

    #[test]
    fn tampering_and_wrong_key_are_rejected() {
        let hello = Hello::new(b"secret", "dev-a", claim(), 1_000);
        assert_eq!(hello.verify(b"other", 1_000), Err(AuthError::Mac));
        let mut tampered = hello.clone();
        tampered.device = "dev-b".into();
        assert_eq!(tampered.verify(b"secret", 1_000), Err(AuthError::Mac));
        let mut tampered = hello.clone();
        tampered.claim.uid = "other".into();
        assert_eq!(tampered.verify(b"secret", 1_000), Err(AuthError::Mac));
        let mut garbage = hello;
        garbage.mac = "zz".into();
        assert_eq!(garbage.verify(b"secret", 1_000), Err(AuthError::Mac));
    }

    #[test]
    fn field_boundaries_are_unambiguous() {
        let a = Hello {
            nonce: "n".into(),
            ..Hello::new(
                b"k",
                "ab",
                ClaimRef {
                    namespace: "c".into(),
                    name: "".into(),
                    uid: "".into(),
                },
                1,
            )
        };
        let b = Hello {
            nonce: "n".into(),
            ..Hello::new(
                b"k",
                "a",
                ClaimRef {
                    namespace: "bc".into(),
                    name: "".into(),
                    uid: "".into(),
                },
                1,
            )
        };
        assert_ne!(
            a.mac_for(b"k").finalize().into_bytes(),
            b.mac_for(b"k").finalize().into_bytes()
        );
    }

    #[test]
    fn stale_hello_is_rejected() {
        let hello = Hello::new(b"secret", "dev", claim(), 1_000);
        assert_eq!(
            hello.verify(b"secret", 1_000 + MAX_CLOCK_SKEW_SECS + 1),
            Err(AuthError::Expired)
        );
    }

    #[test]
    fn nonce_replay_is_rejected() {
        let mut cache = NonceCache::default();
        assert_eq!(cache.check_and_insert("n1", 100, 100), Ok(()));
        assert_eq!(cache.check_and_insert("n1", 100, 101), Err(AuthError::Replay));
        // Entries expire with the skew window.
        assert_eq!(cache.check_and_insert("n1", 100, 100 + MAX_CLOCK_SKEW_SECS + 1), Ok(()));
    }

    #[tokio::test]
    async fn line_reader_does_not_over_read() {
        let mut data: &[u8] = b"{\"ok\":true}\nUSBREDIR-BYTES";
        let reply: Reply = read_line(&mut data).await.unwrap();
        assert_eq!(reply, Reply { ok: true, error: None });
        assert_eq!(data, b"USBREDIR-BYTES");
    }
}
