# Seed 107374182407 Failure RCA

## Summary

- Run: `target/adversarial/one-hour/run-1783606073`
- Seed dir: `target/adversarial/one-hour/run-1783606073/seed-107374182407`
- Mode: deterministic
- Failing op: 85, `BatchQuery`
- Namespace: `test-a1a37c67-867e-4a62-bf25-7f4be1f0927f-adv-107374182407-2`
- Violations: `I5BatchEquivalent` x5

## What Happened

The batch query returned top-level HTTP 200 with five nested batch entries. Each
nested entry had `ok: false` with `INTERNAL_ERROR`, status 500, and
`retryable: false`.

The adversarial runner also issued the same five queries individually. Each
individual query returned HTTP 500 with the same canonical error fields. The
individual error body additionally had a request id, which is expected for the
single-query HTTP error envelope.

## Root Cause

`check_i5_batch_equivalence` only accepted nested errors when the generated query
class was `ExpectError`. Membership-class queries were always treated as success
contracts, so matching nested batch and individual failures were reported as
`batch success entry did not match individual query`.

That made this seed a false positive for I5. The invariant should compare batch
and individual behavior. It should not require a success entry when both paths
return equivalent structured errors.

## Fix

Teach the adversarial oracle to accept equivalent nested batch and individual
errors for non-`ExpectError` query classes. The comparison ignores volatile
single-request fields such as `request_id` and compares status, code, error text,
and retryability.
