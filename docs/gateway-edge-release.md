# Gateway Edge Breaking Release

This is the owner-controlled release handoff for the coordinated Gateway HTTP/WebSocket
edge change. It is a checklist, not evidence that any application was built, signed,
installed, deployed, or smoke-tested.

## Compatibility contract

The release is one atomic first-party component set:

| Compatibility set | Supported |
| --- | --- |
| New Gateway + New Desktop + New Pioneer App | Yes |
| Every other old/new combination | No |

The new contract has one `gateway_base_url`. WebSocket upgrades use root `GET /`, native
storage reads use `/storage/...`, and native requests carry
`Pioneer-Protocol-Version: 1`. `/health` and `/ready` are unversioned operational routes.
There is no legacy path, capability negotiation, mixed-version support window, or runtime
fallback.

Relay deployments must preserve the root WebSocket upgrade, `/storage/...`, Authorization,
the protocol-version header, Range conditionals, streaming, and the configured public base
prefix. The static nginx example in Pioneer Relay documents that edge contract.

## Backup before publication

Before installing any component from this release:

1. Record the exact source revisions and current released component versions.
2. With the applications stopped by the owner, make a recoverable, access-controlled backup
   of the Desktop runtime home, including `gateway-registry.toml`. Keep its platform credential
   storage in the same encrypted system backup; do not export credentials into an ordinary
   archive.
3. Make an encrypted OS/device backup of Pioneer App data. The app migrates the durable
   `pioneer.gateway.registry.v2` value to `pioneer.gateway.registry.v3` and removes the v2
   value only after v3 persistence succeeds.
4. Verify that each backup can be read by its intended recovery mechanism, record a checksum
   for ordinary files, and retain the backup until the coordinated release is accepted.

An unambiguous Desktop v2 registry is atomically rewritten as v3 on first load. Ambiguous
custom WebSocket paths are not guessed and require explicit endpoint reconfiguration.

## Publication and recovery

Publish the Gateway, Desktop, and Pioneer App artifacts as one compatibility set. Do not
roll out only one member of the set and do not advertise compatibility with older clients.

If a defect is found before user data migration, prefer a forward fix across the same component
set. A rollback is valid only when all first-party components are rolled back together and the
matching pre-release local-data backups are restored. Mixed old/new rollback is unsupported;
the Gateway must not gain legacy routes or transports to facilitate it.

## Owner build, signing, and publication manifest

The release owner supplies and records:

- exact Pioneer, Pioneer App, and Pioneer Relay source revisions;
- pinned Rust toolchain and `Cargo.lock`, plus the Pioneer App package/runtime lock inputs;
- generated Rust/FFI/Nitro/TypeScript contracts from the same source set;
- production configuration and the reviewed Relay/proxy template;
- platform signing, notarization, store, and publication credentials through their existing
  protected release systems.

The intended outputs are:

- Gateway/CLI platform bundles and checksums;
- signed Desktop installers and update metadata;
- signed Pioneer App iOS and Android store artifacts;
- reviewed Relay deployment configuration where a Relay edge is used.

After building outside the development machine covered by this handoff, the owner verifies
signatures, notarization/store acceptance, checksums, embedded versions, generated-contract
identity, and artifact inventory before publication. Post-publication validation must use an
isolated release environment, not an already running production Gateway on a developer machine.

## Release-note text

This is a breaking coordinated network update. Update Pioneer Gateway, Pioneer Desktop, and
Pioneer App together. WebSocket connections now upgrade at the Gateway root, file and avatar
reads use authenticated `/storage/...` HTTP routes, and network protocol version 1 is carried in
the request header. Older clients and mixed old/new component combinations are not supported.
