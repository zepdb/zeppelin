# Test emulators for the non-S3 substrates

Zeppelin's GCS and Azure test gates run against native (no-Docker) emulators,
mirroring the MinIO setup in `tests/CLAUDE.md`. Fidelity of both emulators is
asserted by `tests/emulator_fidelity_probe.rs` (probes P1–P11); that probe is
the authority — rerun it after changing an emulator version.

## GCS — patched fake-gcs-server v1.55.1

Stock fake-gcs-server cannot serve `object_store` 0.11.2, which speaks the
GCS XML API: the XML PUT route does not exist upstream
(fsouza/fake-gcs-server#331), and the unmerged PR #1164 ignores
`x-goog-if-generation-match` — CAS tests would pass vacuously. The
operator-approved fix (2026-08-14) is a local patch on the pinned release:

```bash
./build-fake-gcs-server.sh   # → ~/.local/bin/fake-gcs-server-zeppelin
fake-gcs-server-zeppelin -scheme http -host 127.0.0.1 -port 4443 \
  -public-host 127.0.0.1:4443 -backend filesystem -filesystem-root /tmp/fgcs-data
```

`-public-host` must equal the host:port clients dial, or the emulator treats
path-style XML requests as virtual-host-style and 404s. Buckets are created
via the JSON API (`POST /storage/v1/b?project=test`); the fidelity probe and
the test harness create `zeppelin-test` themselves.

Patch contents (`fake-gcs-server-xml-api.patch`, ~100 lines): XML-API
PUT/DELETE object routes; `x-goog-if-generation-match` honored through the
upstream `generationCondition` plumbing (412 on mismatch); `ETag` +
`x-goog-generation` headers on upload responses; quoted ETags in XML list
output (byte-equal to GET, as on real GCS). Retire the patch when upstream
merges XML API support.

## Azure — Azurite (npm)

```bash
npm install -g azurite@3.36.0
azurite-blob --blobHost 127.0.0.1 --blobPort 10000 --location /tmp/azurite-data
```

Stock Azurite 3.36.0 passed every probe. `object_store`'s `use_emulator`
handles the well-known dev account and endpoint automatically
(`AZURITE_BLOB_STORAGE_URL` overrides the default `http://127.0.0.1:10000`).
The probe creates the `zeppelin-test` container with a SharedKey-signed
request.

## Probe invocation

```bash
FIDELITY_PROBE_GCS_ENDPOINT=http://127.0.0.1:4443 \
  cargo test --test emulator_fidelity_probe gcs_ -- --nocapture
FIDELITY_PROBE_AZURE=1 \
  cargo test --test emulator_fidelity_probe azure_ -- --nocapture
```

Unset, the probes skip — plain `cargo test` stays green without emulators.
