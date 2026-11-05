# DuckDB, dbt and Iceberg feasibility experiment

This directory preserves a local experiment against Polaris and SeaweedFS.
Read the [French findings report](REPORT.fr.md) before running the probes.
This is fork-only research, not a supported OpenLakeForge distribution.

## Scope and prerequisites

The initial dbt tests used 2.0.9; later failure tests used 2.0.11.
The dbt driver reported DuckDB 1.5.4. Initial Python tests used 2.0.0;
separate transaction tests then used Python DuckDB 1.5.4.
Exact extension revisions and Polaris/SeaweedFS versions remain to be recorded.

The SQL names and endpoints deliberately match the local test deployment:
catalog `lakehouse_dev`, namespace `duckdb_feasibility_lab`, bucket
`lakehouse-dev-silver`. Adapt them for another deployment before execution.
Do not aim this experiment at production.

Keep two tunnels open in separate terminals:

```bash
kubectl --context kind-openlakeforge-local port-forward -n olf-system svc/polaris 18181:8181
```

```bash
kubectl --context kind-openlakeforge-local port-forward -n olf-system svc/seaweedfs-s3 18333:8333
```

In a third terminal, load `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and a
valid `POLARIS_ACCESS_TOKEN` from your deployment without printing them.
Create an untracked `profiles.yml` from `profiles.example.yml`.
The example contains environment-variable references, not credential values.
Keep your existing uv environment; dbt here is a separate executable.

## Model experiment

Run from this directory:

```bash
dbt debug --profiles-dir . --target iceberg
dbt run --profiles-dir . --target iceberg --select customer_summary
dbt run-operation probe_iceberg --profiles-dir . --target iceberg
```

The checked-in model yields A=4/B=2. Run it twice to exercise replacement.
The creation override chooses a sibling S3 location suffixed by `invocation_id`.
It is restricted to this target and namespace and is not a complete replacement
for all adapter features. The pre-hook explicitly reattaches the catalog;
profile options alone did not provide the intended storage configuration.

The failure model yields A=4/B=3. For the recorded scenario, establish a
baseline without the final `('B', 107)` row, run `transaction_probe`, restore
that row, then run:

```bash
dbt run --profiles-dir . --target iceberg --select transaction_probe --vars '{"fail_after_swap": true}'
```

Expect an intentional post-hook conversion error. Inspect the final and backup
tables before retrying: the recorded failure left the new result published.

## Quality experiment

`quality_lab.sql` preserves three separate operations: prepare an invalid
candidate, correct it, and validate before a two-rename transaction.
The publication macro requires an existing `quality_published_01` baseline
(A=4/B=2) and no `quality_previous_01` backup.
Preparation creates A=5/B=-1; correction changes only B to 3.
Publication must fail before correction and may succeed after correction.
These are stateful probes, not an automatically replayable test suite.

Read-only operations `probe_rename_state` and `probe_quality_previous` inspect
the published and previous quality tables. After the recorded successful
exchange, these were A=5/B=3 and A=4/B=2 respectively.
Do not repeat creation or publication blindly, and do not delete S3 directories
based on a `tmp` name. No automatic file cleanup is included.
Raw local logs, databases and tokens are intentionally excluded.
