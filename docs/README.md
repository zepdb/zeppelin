# Zeppelin documentation

This directory contains reviewed operator and contributor documentation:

- [`security-deployment.md`](security-deployment.md) covers production security
  boundaries and operational verification.
- [`branching-operations.md`](branching-operations.md) covers copy-on-write fork
  operations and release status.
- [`compliance-mapping.md`](compliance-mapping.md) maps implemented mechanisms
  to selected control families without making a certification claim.
- [`late-interaction.md`](late-interaction.md) covers late-interaction query and
  semantic-coverage behavior.
- [`rustdoc-style.md`](rustdoc-style.md) defines contributor documentation
  conventions.
- [`evidence/`](evidence/) holds compact, tracked result tables cited by public
  documentation and rustdoc.

The repository's `tasks/` directory is different: it is a gitignored working
ledger for implementation plans, raw session evidence, and local follow-ups.
Public documentation must not link readers there.
