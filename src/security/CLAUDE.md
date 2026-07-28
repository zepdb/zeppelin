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
- `audit.rs`, `audit_sink.rs`, `audit_chain.rs` — durable audit.
- `delegation.rs` — delegated credentials and object signing.

## Entitlements

`Feature` has 8 variants in a **stable bit-assignment order** (`Feature::ALL`).
Append new variants at the end.

Be precise about what breaks what. A signed license carries feature *names*
(serde `rename_all = "snake_case"`), and `feature_bits` is recomputed from
those names on every boot — it is never persisted or signed. So:

- **Renaming** a variant invalidates every issued license naming that feature.
- **Reordering** does *not* break signature verification. It silently
  reassigns every in-memory bit index, and it breaks the `#[repr(C)]` layout
  mirror that `tests/common/server.rs` transmutes into. Still forbidden — the
  failure is just quieter than a signature error.
- The `u16` mask caps the inventory at **16** features.

> Corrected 2026-07-24. An earlier revision said "reordering invalidates
> existing licenses." That conflates the two failure modes; renaming is what
> invalidates a license.

Enforce entitlements in the **kernel**, not only in the handler — a handler-only
check is easy to bypass from a new call site.

Branching is currently symmetric and kernel-enforced. `Feature::Branching` is
checked in four `kernel.rs` functions: `authorize_namespace_fork`,
`authorize_branch_list`, `fresh_current_fork_authorization`, and
`fresh_loaded_policy_fork_authorization`. The handler-side config flag in
`server/handlers/namespace.rs` is a second, independent gate — not the only
one. Keep any new branch-adjacent authority on the same pattern.

Not every feature is kernel-enforced, though. `Feature::Rbac` is gated **only**
by route selection in `server/mod.rs::security_routes`; the kernel's admin
methods carry no `Rbac` check. The backstop is indirect: an unlicensed boot
builds `SecurityAuthority::Bootstrap`, whose admin arms return
`InvalidPolicyRequest` → 400, not 403 `feature_not_licensed`. Delegation and
preservation *do* re-check their feature in the kernel.

> Corrected 2026-07-24. An earlier revision of this file claimed
> `authorize_branch_list` relied on the handler's config flag alone. That was
> already false when written: the kernel check landed in `4f8583c`. Verify
> against the code before trusting a gap claim here.

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
