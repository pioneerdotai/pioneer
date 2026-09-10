//! Immutable protocol values used at the typed Settings boundary.
pub use pioneer_protocol::{
    AuthMeResponse, AuthPrincipalSnapshot, AuthSessionListItem, AuthSessionStatus,
    AuthorizationCapabilitySnapshot, ClientKind, GatewaySettingsSnapshot,
    PROFILE_AVATAR_MAX_DECODED_BYTES, ProfileAvatarInput,
};
pub use pioneer_protocol::{
    GatewayMemoryModelSelection, GatewayMemorySettings, GatewayRemoteAccessErrorKind,
    GatewayRemoteAccessSettings, GatewayRemoteAccessState, GatewaySelfImprovementModelSelection,
    GatewaySelfImprovementSettings, GatewayThreadEpisodicVectorLocalModelStatus,
    GatewayThreadEpisodicVectorProvider, GatewayThreadEpisodicVectorRefillStatus,
    GatewayThreadEpisodicVectorSearchSettings, GatewayVoiceInputProvider,
    GatewayVoiceInputRuntimePhase, GatewayVoiceInputSettings, ProviderModelInfo, RuntimeSummary,
    SelfImprovementPhase, SelfImprovementStatusReason,
};
