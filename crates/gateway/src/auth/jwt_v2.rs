use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation, decode, encode};
use pioneer_config::GatewayAuthConfig;
use pioneer_protocol::{AuthSessionId, DeviceId, GatewayId, PrincipalId, generate_id};
use serde::{Deserialize, Serialize};

use super::{AuthError, AuthErrorCode};

const JWT_V2: u8 = 2;
const ACCESS_TYPE: &str = "access";
const ACCESS_PURPOSE: &str = "gateway_access";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AccessJwtSubject {
    pub(crate) gateway_id: GatewayId,
    pub(crate) principal_id: PrincipalId,
    pub(crate) device_id: DeviceId,
    pub(crate) session_id: AuthSessionId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AccessCredential {
    pub(crate) subject: AccessJwtSubject,
    pub(crate) jti: String,
    pub(crate) issued_at_unix: u64,
    pub(crate) expires_at_unix: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AccessClaims {
    ver: u8,
    typ: String,
    purpose: String,
    gid: String,
    sub: String,
    did: String,
    sid: String,
    jti: String,
    iss: String,
    aud: String,
    iat: u64,
    nbf: u64,
    exp: u64,
}

#[derive(Debug, Clone)]
struct JwtV2Config {
    issuer: String,
    audience: String,
    access_ttl_seconds: u64,
}

impl JwtV2Config {
    fn from_auth(config: &GatewayAuthConfig) -> Result<Self, AuthError> {
        config
            .validate_session_security()
            .map_err(|_| AuthError::new(AuthErrorCode::InvalidCredential))?;
        Ok(Self {
            issuer: config.jwt_issuer.trim().to_owned(),
            audience: config.jwt_audience.trim().to_owned(),
            access_ttl_seconds: config.access_token_ttl_seconds,
        })
    }

    fn validation(&self) -> Validation {
        let mut validation = Validation::new(Algorithm::HS256);
        validation.set_issuer(&[self.issuer.as_str()]);
        validation.set_audience(&[self.audience.as_str()]);
        validation.validate_exp = false;
        validation.validate_nbf = false;
        validation
    }
}

pub(crate) struct AccessJwtIssuer {
    encoding_key: EncodingKey,
    decoding_key: DecodingKey,
    config: JwtV2Config,
    gateway_id: GatewayId,
}

impl AccessJwtIssuer {
    pub(crate) fn new(
        key: &[u8],
        config: &GatewayAuthConfig,
        gateway_id: GatewayId,
    ) -> Result<Self, AuthError> {
        if key.len() < 32 {
            return Err(AuthError::new(AuthErrorCode::InvalidCredential));
        }
        Ok(Self {
            encoding_key: EncodingKey::from_secret(key),
            decoding_key: DecodingKey::from_secret(key),
            config: JwtV2Config::from_auth(config)?,
            gateway_id,
        })
    }

    pub(crate) fn issue(
        &self,
        subject: &AccessJwtSubject,
        now_unix: u64,
        jti: Option<String>,
    ) -> Result<String, AuthError> {
        if subject.gateway_id != self.gateway_id {
            return Err(AuthError::new(AuthErrorCode::GatewayIdentityMismatch));
        }
        let exp = now_unix
            .checked_add(self.config.access_ttl_seconds)
            .ok_or_else(|| AuthError::new(AuthErrorCode::InvalidCredential))?;
        let claims = AccessClaims {
            ver: JWT_V2,
            typ: ACCESS_TYPE.to_owned(),
            purpose: ACCESS_PURPOSE.to_owned(),
            gid: subject.gateway_id.to_string(),
            sub: subject.principal_id.to_string(),
            did: subject.device_id.to_string(),
            sid: subject.session_id.to_string(),
            jti: jti.unwrap_or_else(|| generate_id(21)),
            iss: self.config.issuer.clone(),
            aud: self.config.audience.clone(),
            iat: now_unix,
            nbf: now_unix,
            exp,
        };
        encode(&Header::new(Algorithm::HS256), &claims, &self.encoding_key)
            .map_err(|_| AuthError::new(AuthErrorCode::InvalidCredential))
    }

    pub(crate) fn validate(
        &self,
        token: &str,
        now_unix: u64,
    ) -> Result<AccessCredential, AuthError> {
        let claims = decode::<AccessClaims>(token, &self.decoding_key, &self.config.validation())
            .map_err(|_| AuthError::new(AuthErrorCode::InvalidCredential))?
            .claims;
        validate_common(
            claims.ver,
            &claims.typ,
            &claims.purpose,
            ACCESS_TYPE,
            ACCESS_PURPOSE,
            claims.iat,
            claims.nbf,
            claims.exp,
            self.config.access_ttl_seconds,
            now_unix,
        )?;
        let gateway_id = GatewayId::new(claims.gid)
            .map_err(|_| AuthError::new(AuthErrorCode::InvalidCredential))?;
        if gateway_id != self.gateway_id {
            return Err(AuthError::new(AuthErrorCode::GatewayIdentityMismatch));
        }
        Ok(AccessCredential {
            subject: AccessJwtSubject {
                gateway_id,
                principal_id: PrincipalId::new(claims.sub)
                    .map_err(|_| AuthError::new(AuthErrorCode::InvalidCredential))?,
                device_id: DeviceId::new(claims.did)
                    .map_err(|_| AuthError::new(AuthErrorCode::InvalidCredential))?,
                session_id: AuthSessionId::new(claims.sid)
                    .map_err(|_| AuthError::new(AuthErrorCode::InvalidCredential))?,
            },
            jti: validate_jti(claims.jti)?,
            issued_at_unix: claims.iat,
            expires_at_unix: claims.exp,
        })
    }
}

#[allow(clippy::too_many_arguments)]
fn validate_common(
    version: u8,
    actual_type: &str,
    actual_purpose: &str,
    expected_type: &str,
    expected_purpose: &str,
    iat: u64,
    nbf: u64,
    exp: u64,
    maximum_ttl: u64,
    now: u64,
) -> Result<(), AuthError> {
    if version != JWT_V2 || actual_type != expected_type || actual_purpose != expected_purpose {
        return Err(AuthError::new(AuthErrorCode::UnsupportedCredential));
    }
    let lifetime = exp
        .checked_sub(iat)
        .ok_or_else(|| AuthError::new(AuthErrorCode::InvalidCredential))?;
    if lifetime == 0 || lifetime > maximum_ttl || nbf < iat || nbf >= exp {
        return Err(AuthError::new(AuthErrorCode::InvalidCredential));
    }
    if now < nbf {
        return Err(AuthError::new(AuthErrorCode::InvalidCredential));
    }
    if now >= exp {
        return Err(AuthError::new(AuthErrorCode::CredentialExpired));
    }
    Ok(())
}

fn validate_jti(jti: String) -> Result<String, AuthError> {
    if jti.len() != 21 || !jti.chars().all(|value| value.is_ascii_alphanumeric()) {
        return Err(AuthError::new(AuthErrorCode::InvalidCredential));
    }
    Ok(jti)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> GatewayAuthConfig {
        GatewayAuthConfig::default()
    }

    fn gateway(value: &str) -> GatewayId {
        GatewayId::new(value).unwrap()
    }

    fn access_claims() -> AccessClaims {
        AccessClaims {
            ver: JWT_V2,
            typ: ACCESS_TYPE.to_owned(),
            purpose: ACCESS_PURPOSE.to_owned(),
            gid: "G00000000000000000001".to_owned(),
            sub: "P00000000000000000001".to_owned(),
            did: "D00000000000000000001".to_owned(),
            sid: "S00000000000000000001".to_owned(),
            jti: "J00000000000000000001".to_owned(),
            iss: config().jwt_issuer,
            aud: config().jwt_audience,
            iat: 1_000,
            nbf: 1_000,
            exp: 1_900,
        }
    }

    fn encode_access(claims: &AccessClaims, key: &[u8]) -> String {
        encode(
            &Header::new(Algorithm::HS256),
            claims,
            &EncodingKey::from_secret(key),
        )
        .unwrap()
    }

    #[test]
    fn access_is_gateway_isolated() {
        let access_key = [1; 64];
        let gateway_id = gateway("G00000000000000000001");
        let principal_id = PrincipalId::new("P00000000000000000001").unwrap();
        let access = AccessJwtIssuer::new(&access_key, &config(), gateway_id.clone()).unwrap();
        let token = access
            .issue(
                &AccessJwtSubject {
                    gateway_id: gateway_id.clone(),
                    principal_id: principal_id.clone(),
                    device_id: DeviceId::new("D00000000000000000001").unwrap(),
                    session_id: AuthSessionId::new("S00000000000000000001").unwrap(),
                },
                1_000,
                Some("J00000000000000000001".to_owned()),
            )
            .unwrap();
        let validated = access.validate(&token, 1_001).unwrap();
        assert_eq!(validated.subject.principal_id, principal_id);

        let other =
            AccessJwtIssuer::new(&access_key, &config(), gateway("G00000000000000000002")).unwrap();
        assert_eq!(
            other.validate(&token, 1_001).unwrap_err().code(),
            AuthErrorCode::GatewayIdentityMismatch
        );
    }

    #[test]
    fn access_claim_contract_rejects_wrong_scope_ids_lifetime_and_clock() {
        let key = [1; 64];
        let issuer =
            AccessJwtIssuer::new(&key, &config(), gateway("G00000000000000000001")).unwrap();
        let valid = access_claims();
        let credential = issuer
            .validate(encode_access(&valid, &key).as_str(), 1_001)
            .unwrap();
        assert_eq!(credential.subject.gateway_id.as_str(), valid.gid);
        assert_eq!(credential.subject.principal_id.as_str(), valid.sub);
        assert_eq!(credential.subject.device_id.as_str(), valid.did);
        assert_eq!(credential.subject.session_id.as_str(), valid.sid);
        assert_eq!(credential.jti, valid.jti);
        assert_eq!(credential.issued_at_unix, valid.iat);
        assert_eq!(credential.expires_at_unix, valid.exp);

        let mutations: Vec<Box<dyn Fn(&mut AccessClaims)>> = vec![
            Box::new(|claims| claims.ver = 3),
            Box::new(|claims| claims.typ = "refresh".to_owned()),
            Box::new(|claims| claims.purpose = "invalid_access_purpose".to_owned()),
            Box::new(|claims| claims.gid = "G00000000000000000002".to_owned()),
            Box::new(|claims| claims.sub = "invalid-principal".to_owned()),
            Box::new(|claims| claims.did = "invalid-device".to_owned()),
            Box::new(|claims| claims.sid = "invalid-session".to_owned()),
            Box::new(|claims| claims.exp = claims.iat + 901),
            Box::new(|claims| claims.nbf = claims.iat + 10),
        ];
        for mutate in mutations {
            let mut claims = access_claims();
            mutate(&mut claims);
            assert!(
                issuer
                    .validate(encode_access(&claims, &key).as_str(), 1_001)
                    .is_err(),
                "mutated access claims must fail closed: {claims:?}"
            );
        }

        assert_eq!(
            issuer
                .validate(encode_access(&valid, &key).as_str(), valid.exp)
                .unwrap_err()
                .code(),
            AuthErrorCode::CredentialExpired
        );
    }

    #[test]
    fn access_signing_key_rotation_invalidates_only_old_access_material() {
        let gateway_id = gateway("G00000000000000000001");
        let old =
            AccessJwtIssuer::new(&[1; 64], &config(), gateway_id.clone()).expect("old issuer");
        let rotated =
            AccessJwtIssuer::new(&[2; 64], &config(), gateway_id).expect("rotated issuer");
        let token = old
            .issue(
                &AccessJwtSubject {
                    gateway_id: gateway("G00000000000000000001"),
                    principal_id: PrincipalId::new("P00000000000000000001").unwrap(),
                    device_id: DeviceId::new("D00000000000000000001").unwrap(),
                    session_id: AuthSessionId::new("S00000000000000000001").unwrap(),
                },
                1_000,
                Some("J00000000000000000001".to_owned()),
            )
            .unwrap();
        assert!(old.validate(&token, 1_001).is_ok());
        assert_eq!(
            rotated.validate(&token, 1_001).unwrap_err().code(),
            AuthErrorCode::InvalidCredential
        );
    }

    #[test]
    fn v2_claim_decoders_reject_unknown_fields() {
        let access_key = [1; 64];
        let access =
            AccessJwtIssuer::new(&access_key, &config(), gateway("G00000000000000000001")).unwrap();
        let mut claims = serde_json::to_value(access_claims()).unwrap();
        claims
            .as_object_mut()
            .unwrap()
            .insert("role".to_owned(), serde_json::json!("superuser"));
        let token = encode(
            &Header::new(Algorithm::HS256),
            &claims,
            &EncodingKey::from_secret(&access_key),
        )
        .unwrap();

        assert_eq!(
            access.validate(&token, 1_001).unwrap_err().code(),
            AuthErrorCode::InvalidCredential
        );
    }
}
