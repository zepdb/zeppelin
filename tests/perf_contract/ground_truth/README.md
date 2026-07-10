# Ground truths

`gt-a.toml` anchors bytes to throughput on the measured local MinIO wall.
`gt-b.toml` validates closed-loop and latency mechanics, with its missing
bytes/query caveat encoded both in the fixture and generated report.

The next vector-db-benchmark campaign remains outside this runner. When it
publishes measured counters and latency together, add that result here as
GT-C rather than silently replacing either existing fixture.
