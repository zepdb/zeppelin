# src/security — kernel, policy, entitlements, audit

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
- `entitlements.rs` / `license.rs` — Ed25519-signed licensed features.
- `preservation.rs` — legal-hold / destruction guards.
- `audit.rs`, `audit_sink.rs`, `audit_chain.rs`, `merkle.rs` — durable audit.
- `delegation.rs`, `receipt.rs` — delegated credentials, signed receipts.

## Entitlements

`Feature` has 9 variants in a **stable bit-assignment order** (`Feature::ALL`).
Append new variants at the end; reordering invalidates existing licenses.

Enforce entitlements in the **kernel**, not only in the handler — a handler-only
check is easy to bypass from a new call site. Current state is inconsistent
here: `authorize_namespace_fork` checks `Feature::Branching` in the kernel,
but `authorize_branch_list` does not and relies on the handler's config flag
alone. Prefer the kernel-side check.

## The policy publication lease and the `Local` backend

`PolicyPublicationLease` acquires create-only and **releases via ETag CAS**.
`object_store`'s `LocalFileSystem` does not implement conditional update, so on
`StorageBackend::Local` the release returns `Storage(NotImplemented)`.

Because `PolicyStore::bootstrap` acquires this lease, **first boot on a
`Local`-backed store currently fails**. That is why these are red without
MinIO:

- `security::policy_publication::tests::missing_acquisition_is_create_only_and_release_keeps_a_cas_record`
- `security::policy_publication::tests::expired_takeover_increments_token_and_stale_release_cannot_overwrite`
- `startup::tests::licensed_file_boot_enables_rbac_routes`

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

Signed-license startup tests need an isolated bucket via
`ZEPPELIN_LICENSE_TEST_BUCKET` when run against MinIO.

## See also

- `../namespace/CLAUDE.md` — governed deletion consumes preservation guards
- `tasks/security/` — the phased security plan
