use std::fmt;
use std::io::{self, Read, Write};
use zeroize::Zeroize;

pub const BRIDGE_FRAME_MAGIC: [u8; 4] = *b"PCMB";
pub const BRIDGE_FRAME_VERSION: u16 = 1;
pub const FRAME_HEADER_BYTES: usize = 12;
pub const MAX_FRAME_PAYLOAD_BYTES: usize = 8 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum BridgeFrameType {
    Attach = 1,
    Payload = 2,
    Cancellation = 3,
    Result = 4,
    Error = 5,
    Shutdown = 6,
}

impl TryFrom<u8> for BridgeFrameType {
    type Error = BridgeFrameError;

    fn try_from(value: u8) -> Result<Self, BridgeFrameError> {
        match value {
            1 => Ok(Self::Attach),
            2 => Ok(Self::Payload),
            3 => Ok(Self::Cancellation),
            4 => Ok(Self::Result),
            5 => Ok(Self::Error),
            6 => Ok(Self::Shutdown),
            other => Err(BridgeFrameError::UnsupportedFrameType(other)),
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct BridgeFrame {
    frame_type: BridgeFrameType,
    payload: Vec<u8>,
}

impl BridgeFrame {
    pub fn new(
        frame_type: BridgeFrameType,
        payload: impl Into<Vec<u8>>,
    ) -> Result<Self, BridgeFrameError> {
        let payload = payload.into();
        validate_payload_len(payload.len(), MAX_FRAME_PAYLOAD_BYTES)?;
        Ok(Self {
            frame_type,
            payload,
        })
    }

    pub const fn frame_type(&self) -> BridgeFrameType {
        self.frame_type
    }

    pub fn payload(&self) -> &[u8] {
        self.payload.as_slice()
    }

    pub fn into_payload(self) -> Vec<u8> {
        self.payload
    }
}

impl fmt::Debug for BridgeFrame {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BridgeFrame")
            .field("frame_type", &self.frame_type)
            .field("payload_bytes", &self.payload.len())
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BridgeFrameError {
    Io(io::ErrorKind),
    InvalidMagic,
    UnsupportedVersion(u16),
    UnsupportedFrameType(u8),
    UnsupportedFlags(u8),
    PayloadTooLarge { declared: usize, max: usize },
    LengthOverflow,
    TruncatedHeader { actual: usize },
    TruncatedPayload { expected: usize, actual: usize },
    TrailingBytes { actual: usize },
}

impl fmt::Display for BridgeFrameError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(kind) => write!(formatter, "CLI MCP bridge frame I/O failed: {kind:?}"),
            Self::InvalidMagic => formatter.write_str("invalid CLI MCP bridge frame magic"),
            Self::UnsupportedVersion(version) => {
                write!(
                    formatter,
                    "unsupported CLI MCP bridge frame version {version}"
                )
            }
            Self::UnsupportedFrameType(frame_type) => {
                write!(
                    formatter,
                    "unsupported CLI MCP bridge frame type {frame_type}"
                )
            }
            Self::UnsupportedFlags(flags) => {
                write!(formatter, "unsupported CLI MCP bridge frame flags {flags}")
            }
            Self::PayloadTooLarge { declared, max } => write!(
                formatter,
                "CLI MCP bridge frame payload is too large: {declared} > {max}"
            ),
            Self::LengthOverflow => {
                formatter.write_str("CLI MCP bridge frame length exceeds wire representation")
            }
            Self::TruncatedHeader { actual } => {
                write!(
                    formatter,
                    "truncated CLI MCP bridge frame header: {actual} bytes"
                )
            }
            Self::TruncatedPayload { expected, actual } => write!(
                formatter,
                "truncated CLI MCP bridge frame payload: expected {expected}, got {actual}"
            ),
            Self::TrailingBytes { actual } => {
                write!(
                    formatter,
                    "CLI MCP bridge frame has {actual} trailing bytes"
                )
            }
        }
    }
}

impl std::error::Error for BridgeFrameError {}

pub fn encode_frame(frame: &BridgeFrame) -> Result<Vec<u8>, BridgeFrameError> {
    validate_payload_len(frame.payload.len(), MAX_FRAME_PAYLOAD_BYTES)?;
    let payload_len =
        u32::try_from(frame.payload.len()).map_err(|_| BridgeFrameError::LengthOverflow)?;
    let total_len = FRAME_HEADER_BYTES
        .checked_add(frame.payload.len())
        .ok_or(BridgeFrameError::LengthOverflow)?;
    let mut encoded = Vec::with_capacity(total_len);
    encoded.extend_from_slice(&BRIDGE_FRAME_MAGIC);
    encoded.extend_from_slice(&BRIDGE_FRAME_VERSION.to_be_bytes());
    encoded.push(frame.frame_type as u8);
    encoded.push(0);
    encoded.extend_from_slice(&payload_len.to_be_bytes());
    encoded.extend_from_slice(frame.payload.as_slice());
    Ok(encoded)
}

pub fn decode_frame(bytes: &[u8]) -> Result<BridgeFrame, BridgeFrameError> {
    decode_frame_with_limit(bytes, MAX_FRAME_PAYLOAD_BYTES)
}

pub fn decode_frame_with_limit(
    bytes: &[u8],
    max_payload_bytes: usize,
) -> Result<BridgeFrame, BridgeFrameError> {
    if bytes.len() < FRAME_HEADER_BYTES {
        return Err(BridgeFrameError::TruncatedHeader {
            actual: bytes.len(),
        });
    }
    let (frame_type, payload_len) = decode_header(&bytes[..FRAME_HEADER_BYTES], max_payload_bytes)?;
    let expected_len = FRAME_HEADER_BYTES
        .checked_add(payload_len)
        .ok_or(BridgeFrameError::LengthOverflow)?;
    if bytes.len() < expected_len {
        return Err(BridgeFrameError::TruncatedPayload {
            expected: payload_len,
            actual: bytes.len() - FRAME_HEADER_BYTES,
        });
    }
    if bytes.len() > expected_len {
        return Err(BridgeFrameError::TrailingBytes {
            actual: bytes.len() - expected_len,
        });
    }
    BridgeFrame::new(frame_type, bytes[FRAME_HEADER_BYTES..expected_len].to_vec())
}

pub fn read_frame(reader: &mut impl Read) -> Result<Option<BridgeFrame>, BridgeFrameError> {
    read_frame_with_limit(reader, MAX_FRAME_PAYLOAD_BYTES)
}

pub fn read_frame_with_limit(
    reader: &mut impl Read,
    max_payload_bytes: usize,
) -> Result<Option<BridgeFrame>, BridgeFrameError> {
    let mut header = [0_u8; FRAME_HEADER_BYTES];
    let mut header_read = 0;
    while header_read < FRAME_HEADER_BYTES {
        match reader.read(&mut header[header_read..]) {
            Ok(0) if header_read == 0 => return Ok(None),
            Ok(0) => {
                return Err(BridgeFrameError::TruncatedHeader {
                    actual: header_read,
                });
            }
            Ok(read) => header_read += read,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(BridgeFrameError::Io(error.kind())),
        }
    }
    let (frame_type, payload_len) = decode_header(&header, max_payload_bytes)?;
    let mut payload = vec![0_u8; payload_len];
    let mut payload_read = 0;
    while payload_read < payload_len {
        match reader.read(&mut payload[payload_read..]) {
            Ok(0) => {
                return Err(BridgeFrameError::TruncatedPayload {
                    expected: payload_len,
                    actual: payload_read,
                });
            }
            Ok(read) => payload_read += read,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(BridgeFrameError::Io(error.kind())),
        }
    }
    BridgeFrame::new(frame_type, payload).map(Some)
}

pub fn write_frame(writer: &mut impl Write, frame: &BridgeFrame) -> Result<(), BridgeFrameError> {
    let mut encoded = encode_frame(frame)?;
    let result = writer
        .write_all(encoded.as_slice())
        .and_then(|()| writer.flush())
        .map_err(|error| BridgeFrameError::Io(error.kind()));
    encoded.zeroize();
    result
}

pub(crate) fn decode_header(
    header: &[u8],
    max_payload_bytes: usize,
) -> Result<(BridgeFrameType, usize), BridgeFrameError> {
    if header[..4] != BRIDGE_FRAME_MAGIC {
        return Err(BridgeFrameError::InvalidMagic);
    }
    let version = u16::from_be_bytes([header[4], header[5]]);
    if version != BRIDGE_FRAME_VERSION {
        return Err(BridgeFrameError::UnsupportedVersion(version));
    }
    let frame_type = BridgeFrameType::try_from(header[6])?;
    if header[7] != 0 {
        return Err(BridgeFrameError::UnsupportedFlags(header[7]));
    }
    let payload_len = u32::from_be_bytes([header[8], header[9], header[10], header[11]]);
    let payload_len = usize::try_from(payload_len).map_err(|_| BridgeFrameError::LengthOverflow)?;
    validate_payload_len(payload_len, effective_limit(max_payload_bytes))?;
    Ok((frame_type, payload_len))
}

fn effective_limit(configured: usize) -> usize {
    configured.min(MAX_FRAME_PAYLOAD_BYTES)
}

fn validate_payload_len(payload_len: usize, max: usize) -> Result<(), BridgeFrameError> {
    if payload_len > max {
        return Err(BridgeFrameError::PayloadTooLarge {
            declared: payload_len,
            max,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn sample_frame(frame_type: BridgeFrameType) -> BridgeFrame {
        BridgeFrame::new(frame_type, b"bootstrap-secret-canary".to_vec())
            .expect("sample frame should be valid")
    }

    #[test]
    fn framing_round_trip_is_stable_big_endian_wire_format() {
        for frame_type in [
            BridgeFrameType::Attach,
            BridgeFrameType::Payload,
            BridgeFrameType::Cancellation,
            BridgeFrameType::Result,
            BridgeFrameType::Error,
            BridgeFrameType::Shutdown,
        ] {
            let frame = sample_frame(frame_type);
            let encoded = encode_frame(&frame).expect("frame should encode");

            assert_eq!(&encoded[..4], &BRIDGE_FRAME_MAGIC);
            assert_eq!(&encoded[4..6], &BRIDGE_FRAME_VERSION.to_be_bytes());
            assert_eq!(
                &encoded[8..12],
                &(frame.payload().len() as u32).to_be_bytes()
            );
            assert_eq!(decode_frame(encoded.as_slice()), Ok(frame.clone()));

            let mut stream = Cursor::new(Vec::new());
            write_frame(&mut stream, &frame).expect("frame should write");
            stream.set_position(0);
            assert_eq!(read_frame(&mut stream), Ok(Some(frame)));
            assert_eq!(read_frame(&mut stream), Ok(None));
        }
    }

    #[test]
    fn framing_rejects_bad_magic_version_type_flags_and_length() {
        let encoded = encode_frame(&sample_frame(BridgeFrameType::Payload)).expect("valid frame");

        let mut bad_magic = encoded.clone();
        bad_magic[0] ^= 0xff;
        assert_eq!(
            decode_frame(&bad_magic),
            Err(BridgeFrameError::InvalidMagic)
        );

        let mut bad_version = encoded.clone();
        bad_version[4..6].copy_from_slice(&2_u16.to_be_bytes());
        assert_eq!(
            decode_frame(&bad_version),
            Err(BridgeFrameError::UnsupportedVersion(2))
        );

        let mut bad_type = encoded.clone();
        bad_type[6] = 255;
        assert_eq!(
            decode_frame(&bad_type),
            Err(BridgeFrameError::UnsupportedFrameType(255))
        );

        let mut bad_flags = encoded.clone();
        bad_flags[7] = 1;
        assert_eq!(
            decode_frame(&bad_flags),
            Err(BridgeFrameError::UnsupportedFlags(1))
        );

        let mut oversized = encoded;
        oversized[8..12].copy_from_slice(&u32::MAX.to_be_bytes());
        assert_eq!(
            decode_frame_with_limit(&oversized, 128),
            Err(BridgeFrameError::PayloadTooLarge {
                declared: u32::MAX as usize,
                max: 128,
            })
        );
    }

    #[test]
    fn framing_rejects_truncation_and_trailing_corruption() {
        let encoded = encode_frame(&sample_frame(BridgeFrameType::Result)).expect("valid frame");

        assert_eq!(
            decode_frame(&encoded[..FRAME_HEADER_BYTES - 1]),
            Err(BridgeFrameError::TruncatedHeader {
                actual: FRAME_HEADER_BYTES - 1,
            })
        );
        assert!(matches!(
            decode_frame(&encoded[..encoded.len() - 1]),
            Err(BridgeFrameError::TruncatedPayload { .. })
        ));
        let mut trailing = encoded;
        trailing.push(0xff);
        assert_eq!(
            decode_frame(&trailing),
            Err(BridgeFrameError::TrailingBytes { actual: 1 })
        );
    }

    #[test]
    fn framing_stream_rejects_oversize_before_payload_allocation() {
        let mut header = Vec::new();
        header.extend_from_slice(&BRIDGE_FRAME_MAGIC);
        header.extend_from_slice(&BRIDGE_FRAME_VERSION.to_be_bytes());
        header.push(BridgeFrameType::Payload as u8);
        header.push(0);
        header.extend_from_slice(&1024_u32.to_be_bytes());
        let mut stream = Cursor::new(header);

        assert_eq!(
            read_frame_with_limit(&mut stream, 64),
            Err(BridgeFrameError::PayloadTooLarge {
                declared: 1024,
                max: 64,
            })
        );
    }

    #[test]
    fn framing_debug_never_exposes_payload() {
        let frame = sample_frame(BridgeFrameType::Attach);
        let debug = format!("{frame:?}");

        assert!(!debug.contains("bootstrap-secret-canary"));
        assert!(debug.contains("payload_bytes"));
    }
}
