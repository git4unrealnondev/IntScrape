use std::collections::HashMap;

use aes::Aes128;
use cipher::{BlockDecryptMut, BlockEncryptMut, KeyIvInit};
use json::Value;
use serde::{Deserialize, Serialize};

use crate::fingerprint::NodeFingerprint;
use crate::Result;

/// Represents the node's attributes.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct NodeAttributes {
    /// The name of the node.
    #[serde(rename = "n")]
    pub name: String,
    /// The encoded fingerprint for the node.
    #[serde(rename = "c", skip_serializing_if = "Option::is_none")]
    pub fingerprint: Option<String>,
    /// The last modified date of the node.
    #[serde(rename = "t", skip_serializing_if = "Option::is_none")]
    pub modified_at: Option<i64>,
    /// Catch-all for the remaining fields (if any).
    #[serde(flatten)]
    pub other: HashMap<String, Value>,
}

impl NodeAttributes {
    pub(crate) fn decrypt_and_unpack(file_key: &[u8; 16], buffer: &mut [u8]) -> Result<Self> {
        // Public-folder responses may contain entries for which a candidate
        // key is not valid. Reject malformed ciphertext instead of allowing
        // the parser to panic while probing keys.
        if buffer.len() < 16 || buffer.len() % 16 != 0 {
            return Err(crate::Error::InvalidResponseFormat);
        }

        let mut cbc = cbc::Decryptor::<Aes128>::new(file_key.into(), &<_>::default());
        for chunk in buffer.chunks_exact_mut(16) {
            cbc.decrypt_block_mut(chunk.into());
        }

        if buffer.len() < 4 || &buffer[..4] != b"MEGA" {
            return Err(crate::Error::InvalidResponseFormat);
        }

        let payload_end = buffer[4..]
            .iter()
            .position(|byte| *byte == b'\0')
            .map(|offset| offset + 4)
            .unwrap_or(buffer.len());

        if payload_end <= 4 {
            return Err(crate::Error::InvalidResponseFormat);
        }

        let attrs = json::from_slice(&buffer[4..payload_end])?;

        Ok(attrs)
    }

    pub(crate) fn pack_and_encrypt(&self, file_key: &[u8; 16]) -> Result<Vec<u8>> {
        let mut buffer = b"MEGA".to_vec();
        json::to_writer(&mut buffer, self)?;

        let padding_len = (16 - buffer.len() % 16) % 16;
        buffer.extend(std::iter::repeat(b'\0').take(padding_len));

        let mut cbc = cbc::Encryptor::<Aes128>::new(file_key.into(), &<_>::default());
        for chunk in buffer.chunks_exact_mut(16) {
            cbc.encrypt_block_mut(chunk.into());
        }

        Ok(buffer)
    }

    pub(crate) fn extract_fingerprint(&self) -> Option<NodeFingerprint> {
        let checksum = self.fingerprint.as_deref()?;
        NodeFingerprint::deserialize(checksum)
    }
}
