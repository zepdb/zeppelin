# src/security — kernel, policy, audit

Everything here is fail-closed by design. When in doubt, deny and return a
typed error; do not add a permissive default.

## Layers

- `kernel.rs` — the admission point. Evaluates one exhaustive `Action` against
  a typed resource and returns an explicit decision **before** domain work
  starts. Handlers call `authorize_*` / `guard_*` here.
- `policy.rs`, `policy_store.rs`, `policy_cache.rs` — the authoritative policy
  document in `_security/`, its S3-backed head, and the read cache.
- `policy_publication.rs` — the global publication lease (fencing token +
  ETag CAS). Added by the branching activation work.
- `preservation.rs` — legal-hold / destruction guards.
- `audit.rs`, `audit_sink.rs`, `audit_chain.rs` — durable audit.
- `delegation.rs` — delegated credentials and object signing.

## Configured composition

Licensing was removed in 2026-08; no feature is gated behind a license. The
kernel's composition is driven entirely by config (`SecurityKernel::compose`):

- `security.rbac = true` (requires `mode = "enforced"`) selects the
  S3-authoritative policy store; false keeps bootstrap api-key grants. The
  RBAC routes are always registered with real handlers — on a bootstrap boot
  the kernel's admin methods return `FeatureDisabled("rbac")` → 403
  `feature_disabled`.
- Delegation composes exactly when `security.token_signing_key_path` is
  non-empty on an rbac boot. An empty path means "delegation off", not a boot
  error; a *bad* key file is still a loud boot failure.
- Preservation always composes on the rbac path and never under bootstrap
  authority (bootstrap `*` grants carry the preservation actions, but the
  service is absent, so guards return unlocked and the endpoints return
  `FeatureDisabled("preservation")`).
- Durable audit is gated purely by `security.audit_s3` (config validation
  requires a signing key with it; startup requires a backend whose declared
  `StorageCapabilities` include conditional PUT — S3/MinIO, GCS, and Azure
  qualify, `Local` does not).

Enforce feature availability in the **kernel**, not only in the handler — a
handler-only check is easy to bypass from a new call site. Delegation and
preservation check their composed service in the kernel; RBAC administration
rejects on the `SecurityAuthority::Bootstrap` arm.

Branching has no kernel-side gate anymore; `config.branching.enabled` (route
registration + handler re-check) is the only gate. If a new branch-adjacent
surface needs authority beyond namespace grants, add a kernel check rather
than a handler check.

## The policy publication lease and the `Local` backend

`PolicyPublicationLease` acquires create-only, then **renews and releases via
ETag CAS**. `object_store`'s `LocalFileSystem` does not implement conditional
update, so on `StorageBackend::Local` every conditional PUT returns
`Storage(NotImplemented)`.

Because `PolicyStore::bootstrap` acquires this lease, **first boot on a
`Local`-backed store currently fails**. The failure is on **renew**, not
release: `publish_bootstrap_claimed` propagates `publication_lease.renew(..)?`,
so bootstrap cannot renew the lease it just acquired. Release could not cause
this — `release_publication_best_effort` only logs a warning. If you are
debugging a Local-backend boot failure, look at renew first.

That is why these are red without MinIO:

- `security::policy_publication::tests::missing_acquisition_is_create_only_and_release_keeps_a_cas_record`
- `security::policy_publication::tests::expired_takeover_increments_token_and_stale_release_cannot_overwrite`

(`startup::tests::rbac_config_boot_enables_rbac_routes` needs MinIO too, but
skips rather than fails on other backends.)

`Local` is documented as development/testing only, so production (S3/MinIO) is
unaffected — but do not assume `cargo test --lib` being red means your change
broke something. Check against this list first. See `../storage/CLAUDE.md`.

## Audit

Audit delivery is durable and blocking where the policy says it must be. A
failed audit writer marks `/readyz` unavailable — that is intentional, not a
bug. Do not make audit best-effort to "fix" a readiness failure.

`AuditParams` carries the typed shape per action; adding an action means adding
its params variant and its route-map entry.

## Testing

```bash
TEST_BACKEND=minio cargo test --test security_branching_tests \
  --test security_policy_tests --test security_preservation_tests
```

The rbac startup test needs an isolated bucket via
`ZEPPELIN_RBAC_TEST_BUCKET` when run against MinIO.

## See also

- `../namespace/CLAUDE.md` — governed deletion consumes preservation guards
- `tasks/security/` — the phased security plan
