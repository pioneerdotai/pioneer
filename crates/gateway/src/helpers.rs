use anyhow::{Context, Result, anyhow, bail};
use std::time::{SystemTime, UNIX_EPOCH};

pub(crate) fn unix_timestamp_secs() -> Result<u64> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before UNIX epoch")?
        .as_secs())
}

pub(crate) fn normalize_non_empty(value: &str, error_message: &str) -> Result<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        bail!("{error_message}");
    }
    Ok(trimmed.to_owned())
}

pub(crate) fn decode_hex(input: &str) -> Result<Vec<u8>> {
    let input = input.trim();
    if input.is_empty() {
        bail!("hex string is empty");
    }
    if input.len() % 2 != 0 {
        bail!("hex string length must be even");
    }

    let mut bytes = Vec::with_capacity(input.len() / 2);
    let raw = input.as_bytes();
    let mut index = 0;

    while index < raw.len() {
        let high = hex_value(raw[index]).ok_or_else(|| anyhow!("invalid hex digit"))?;
        let low = hex_value(raw[index + 1]).ok_or_else(|| anyhow!("invalid hex digit"))?;
        bytes.push((high << 4) | low);
        index += 2;
    }

    Ok(bytes)
}

pub(crate) fn encode_hex(input: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";

    let mut output = String::with_capacity(input.len() * 2);
    for byte in input {
        output.push(char::from(HEX[(byte >> 4) as usize]));
        output.push(char::from(HEX[(byte & 0x0f) as usize]));
    }

    output
}

fn hex_value(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}
