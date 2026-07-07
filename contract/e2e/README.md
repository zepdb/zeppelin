# Client E2E Conformance

`run.sh` boots MinIO with the engine repo's existing
`docker-compose.test.yml`, starts the local `zeppelin` binary against that
bucket, waits for `/readyz`, then runs:

- Python live integration tests from `../zepdb/zeppelin-py`
- TypeScript live integration tests from `../zepdb/zeppelin-typescript`
- A cross-language parity query against one shared namespace

Usage:

```sh
contract/e2e/run.sh
```

Override repo paths when needed:

```sh
ZEPPELIN_PY_REPO=/path/to/zeppelin-py \
ZEPPELIN_TS_REPO=/path/to/zeppelin-typescript \
contract/e2e/run.sh
```

The runner sets `ZEPPELIN_MAX_BATCH_SIZE=8` for the engine and
`ZEPPELIN_E2E_MAX_BATCH_SIZE=8` for the clients so the 413 oversize-upsert
case is deterministic without sending a large request body.

By default MinIO is exposed on local ports `19000`/`19001` to avoid
colliding with a developer MinIO already listening on `9000`/`9001`.
Override with `ZEPPELIN_E2E_MINIO_PORT` and
`ZEPPELIN_E2E_MINIO_CONSOLE_PORT`.
