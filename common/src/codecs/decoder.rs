use tokio_util::codec::{self, Decoder};

use crate::models::{Batch, Message};

pub struct MessageDecoder {}

impl Decoder for MessageDecoder {
    type Item = Message;

    type Error = std::io::Error;

    fn decode(&mut self, src: &mut bytes::BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let mut codec = codec::LengthDelimitedCodec::default();
        match codec.decode(src) {
            Ok(Some(encoded_data)) => {
                let decoded_data: Message = bincode::deserialize(&encoded_data).unwrap();
                Ok(Some(decoded_data))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(e),
        }
    }
}

pub struct BatchDecoder {}

impl Decoder for BatchDecoder {
    type Item = Batch;

    type Error = std::io::Error;

    fn decode(&mut self, src: &mut bytes::BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let mut codec = codec::LengthDelimitedCodec::default();
        match codec.decode(src) {
            Ok(Some(encoded_data)) => {
                let decoded_data: Batch = bincode::deserialize(&encoded_data).unwrap();
                Ok(Some(decoded_data))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(e),
        }
    }

    fn decode_eof(&mut self, buf: &mut bytes::BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        match self.decode(buf)? {
            Some(frame) => Ok(Some(frame)),
            None => {
                if buf.is_empty() {
                    Ok(None)
                } else {
                    Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "bytes remaining on stream",
                    ))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use crate::codecs::encoder::{BatchEncoder, MessageEncoder};

    use super::*;
    use bytes::BytesMut;
    use tokio_util::codec::Encoder;

    #[test]
    fn test_message_decoder() {
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
        let mut src = BytesMut::new();
        encoder.encode(message.clone(), &mut src).unwrap();

        let mut decoder = MessageDecoder {};

        let decoded = decoder.decode(&mut src).unwrap().unwrap();
        assert_eq!(decoded, message);
    }

    #[test]
    fn test_batch_decoder() {
        let batch = Batch {
            records: vec![
                Message::new(
                    vec![1, 2, 3].into(),
                    None,
                    Some(
                        SystemTime::now()
                            .duration_since(SystemTime::UNIX_EPOCH)
                            .unwrap()
                            .as_millis(),
                    ),
                ),
                Message::new(
                    vec![4, 5, 6].into(),
                    None,
                    Some(
                        SystemTime::now()
                            .duration_since(SystemTime::UNIX_EPOCH)
                            .unwrap()
                            .as_millis(),
                    ),
                ),
            ],
        };
        let mut batch_encoder = BatchEncoder {};
        let mut encoded_batch_buffer = BytesMut::new();
        batch_encoder
            .encode(batch.clone(), &mut encoded_batch_buffer)
            .unwrap();

        let mut decoder = BatchDecoder {};
        let decoded_batch = decoder
            .decode(&mut encoded_batch_buffer.clone())
            .unwrap()
            .unwrap();
        assert_eq!(decoded_batch, batch);
    }
}
