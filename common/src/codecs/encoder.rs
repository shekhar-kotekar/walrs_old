use crate::models::{Batch, Message};
use tokio_util::codec::{Encoder, LengthDelimitedCodec};

pub struct MessageEncoder {
    pub payload_max_bytes: usize,
}

impl Encoder<Message> for MessageEncoder {
    type Error = std::io::Error;

    fn encode(&mut self, message: Message, dst: &mut bytes::BytesMut) -> Result<(), Self::Error> {
        if message.payload.len() > self.payload_max_bytes {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "Cannot encode message. Max payload size allowed is {} bytes.",
                    self.payload_max_bytes
                ),
            ));
        }
        let encoded_data = bincode::serialize(&message).map_err(|err| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, err.to_string())
        })?;
        LengthDelimitedCodec::default().encode(encoded_data.into(), dst)
    }
}

pub struct BatchEncoder {}

impl Encoder<Batch> for BatchEncoder {
    type Error = std::io::Error;

    fn encode(&mut self, item: Batch, dst: &mut bytes::BytesMut) -> Result<(), Self::Error> {
        let encoded_data = bincode::serialize(&item).map_err(|err| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, err.to_string())
        })?;
        LengthDelimitedCodec::default().encode(encoded_data.into(), dst)
    }
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use crate::codecs::decoder::MessageDecoder;

    use super::*;
    use bytes::BytesMut;
    use tokio_util::codec::Decoder;

    #[test]
    fn test_encode() {
        let message = Message::new(
            vec![1, 2, 3].into(),
            None,
            Some(
                SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_millis(),
            ),
        );
        let mut encoder = MessageEncoder {
            payload_max_bytes: 10,
        };
        let mut dst = BytesMut::new();
        encoder.encode(message.clone(), &mut dst).unwrap();

        let mut decoder = MessageDecoder {};
        let decoded = decoder.decode(&mut dst).unwrap().unwrap();
        assert_eq!(decoded, message);
    }

    #[ignore]
    #[test]
    fn test_encode_payload_too_large() {
        let message = Message::new(
            vec![0, 99].into(),
            None,
            Some(
                SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_millis(),
            ),
        );
        let mut encoder = MessageEncoder {
            payload_max_bytes: 10,
        };
        let mut dst = BytesMut::new();
        let result = encoder.encode(message, &mut dst);
        assert!(result.is_err());
    }
}
