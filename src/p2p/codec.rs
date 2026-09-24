// Basic codec that allows us to send and receive messages over gossipsub.
// For now, we only have a single byte identifier and the message content.
// The reason is that we only support sharing audio packets and metadata.

use std::str::from_utf8;
use thiserror::Error;

#[derive(Debug)]
pub enum Message {
    AudioPacket(Vec<u8>),
    Metadata(Metadata),
}

#[derive(Debug)]
pub struct Metadata {
    // stream label
    pub(crate) title: String,
}

#[derive(Debug, Error)]
pub enum DecodeError {
    #[error("Invalid message type")]
    InvalidMessageType,
}

impl Message {
    pub fn encode(self) -> Vec<u8> {
        let mut encoded = Vec::new();
        match self {
            Message::AudioPacket(data) => {
                encoded.push(0);
                encoded.extend_from_slice(&data)
            }
            Message::Metadata(metadata) => {
                encoded.push(1);
                encoded.extend_from_slice(&metadata.encode_bin())
            }
        }
        encoded
    }

    pub fn decode(encoded: Vec<u8>) -> Result<Self, DecodeError> {
        match encoded.first() {
            Some(0) => Ok(Message::AudioPacket(encoded[1..].to_vec())),
            Some(1) => Ok(Message::Metadata(Metadata::decode_bin(&encoded[1..])?)),
            _ => Err(DecodeError::InvalidMessageType),
        }
    }
}

impl Metadata {
    pub fn encode_bin(&self) -> Vec<u8> {
        let mut encoded = Vec::new();
        encoded.extend_from_slice(self.title.as_bytes());
        encoded
    }

    pub fn decode_bin(encoded: &[u8]) -> Result<Self, DecodeError> {
        let title = from_utf8(encoded).map_err(|_| DecodeError::InvalidMessageType)?;
        Ok(Metadata {
            title: title.to_string(),
        })
    }
}
