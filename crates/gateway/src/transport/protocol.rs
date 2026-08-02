use axum::http::HeaderMap;

pub(crate) use pioneer_protocol::{
    PIONEER_PROTOCOL_VERSION, PIONEER_PROTOCOL_VERSION_HEADER,
    PIONEER_PROTOCOL_VERSION_NUMBER,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct InvalidProtocolVersion;

pub(crate) fn validate_protocol_version(
    headers: &HeaderMap,
) -> Result<(), InvalidProtocolVersion> {
    let values = headers
        .get_all(PIONEER_PROTOCOL_VERSION_HEADER)
        .iter()
        .collect::<Vec<_>>();
    if values.len() != 1
        || values[0].as_bytes() != PIONEER_PROTOCOL_VERSION.as_bytes()
    {
        return Err(InvalidProtocolVersion);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use axum::http::{HeaderMap, HeaderValue};

    use super::*;

    #[test]
    fn native_protocol_version_is_exact_single_and_header_only() {
        let mut headers = HeaderMap::new();
        headers.insert(
            PIONEER_PROTOCOL_VERSION_HEADER,
            HeaderValue::from_static(PIONEER_PROTOCOL_VERSION),
        );
        assert!(validate_protocol_version(&headers).is_ok());

        headers.append(
            PIONEER_PROTOCOL_VERSION_HEADER,
            HeaderValue::from_static(PIONEER_PROTOCOL_VERSION),
        );
        assert_eq!(validate_protocol_version(&headers), Err(InvalidProtocolVersion));

        headers.remove(PIONEER_PROTOCOL_VERSION_HEADER);
        assert_eq!(validate_protocol_version(&headers), Err(InvalidProtocolVersion));

        for unsupported in ["01", "2", "v1", "1,1"] {
            let mut headers = HeaderMap::new();
            headers.insert(
                PIONEER_PROTOCOL_VERSION_HEADER,
                HeaderValue::from_str(unsupported).expect("valid test header bytes"),
            );
            assert_eq!(validate_protocol_version(&headers), Err(InvalidProtocolVersion));
        }
    }
}
