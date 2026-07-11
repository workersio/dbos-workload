# Portable serialization writes `NaN` / `Infinity` into `workflow_status.inputs`, so the stored input is not valid JSON

Using the portable JSON serializer, I called a workflow with a `float('nan')`
argument (also happens with `float('inf')` / `float('-inf')`). I expected either
a modeled error or a standard-JSON encoding of the input. Instead the workflow
completes with status `SUCCESS`, and the durable `dbos.workflow_status.inputs`
column for it is stored as `{"namedArgs":{},"positionalArgs":[NaN]}`. `NaN` is
not part of JSON (RFC 8259), so anything that reads that column with a
conforming JSON parser — a workflow started in another language, a `DBOSClient`,
or a plain `JSON.parse` — fails on it. A normal float like `1.5` stores fine, so
the input is silently unreadable only for the float edge cases.

## Environment

- `dbos` 2.26.0 (latest PyPI release); also reproduces on current `main`.
- Python 3.12.3, Postgres 16.
- Portable serialization selected with
  `serialization_type=WorkflowSerializationFormat.PORTABLE`.

## Reproduction

```
pip install "dbos==2.26.0" "psycopg[binary]" "sqlalchemy"
PG_URL="postgresql://user:pass@host:5432/postgres" python repro.py
```

<details><summary>repro.py</summary>

```python
import json, os, uuid
import sqlalchemy as sa
from sqlalchemy.engine import make_url
from dbos import DBOS, DBOSConfig, SetWorkflowID
from dbos._serialization import WorkflowSerializationFormat

PG_URL = os.environ.get("PG_URL", "postgresql://postgres:postgres@localhost:5432/postgres")

def strict_json_ok(text):
    def reject(tok): raise ValueError(tok)
    try:
        json.loads(text, parse_constant=reject); return True
    except ValueError:
        return False

def make_dbs(prefix):
    base = make_url(PG_URL).set(drivername="postgresql+psycopg")
    app_db, sys_db = f"{prefix}_app", f"{prefix}_sys"
    admin = base.set(database="postgres").render_as_string(hide_password=False)
    eng = sa.create_engine(admin, connect_args={"connect_timeout": 5})
    with eng.connect() as raw:
        c = raw.execution_options(isolation_level="AUTOCOMMIT")
        for db in (app_db, sys_db):
            c.execute(sa.text(f'DROP DATABASE IF EXISTS "{db}" WITH (FORCE)'))
            c.execute(sa.text(f'CREATE DATABASE "{db}"'))
    eng.dispose()
    return (base.set(drivername="postgresql", database=app_db).render_as_string(hide_password=False),
            base.set(drivername="postgresql+psycopg", database=sys_db).render_as_string(hide_password=False))

@DBOS.workflow(name="echo", serialization_type=WorkflowSerializationFormat.PORTABLE)
def echo(value):
    return value

def stored_inputs(sys_url, wid):
    eng = sa.create_engine(sys_url, connect_args={"connect_timeout": 5})
    with eng.connect() as c:
        return c.execute(sa.text(
            "SELECT inputs FROM dbos.workflow_status WHERE workflow_uuid = :w"), {"w": wid}).scalar()

def main():
    app_url, sys_url = make_dbs(f"portable_repro_{uuid.uuid4().hex[:8]}")
    config: DBOSConfig = {"name": "portable-repro", "application_database_url": app_url,
                          "system_database_url": sys_url, "serializer": None, "enable_otlp": False}
    DBOS.destroy(destroy_registry=False); DBOS(config=config); DBOS.launch()
    reproduced = False
    try:
        for label, value in [("control float 1.5", 1.5), ("float('nan')", float("nan"))]:
            wid = f"echo-{uuid.uuid4().hex[:8]}"
            with SetWorkflowID(wid):
                echo(value)
            status = DBOS.retrieve_workflow(wid).get_status().status
            stored = stored_inputs(sys_url, wid)
            ok = strict_json_ok(stored)
            print(f"\n{label}:\n  status: {status}\n  stored inputs: {stored}\n  valid JSON: {ok}")
            if status == "SUCCESS" and not ok:
                reproduced = True
    finally:
        DBOS.destroy(destroy_registry=False)
    if reproduced:
        print("\nREPRODUCES: workflow SUCCEEDED but its durable inputs column is not valid JSON")
        return 1
    print("\ndid not reproduce"); return 0

if __name__ == "__main__":
    raise SystemExit(main())
```

</details>

## Observed output

```
control float 1.5:
  status: SUCCESS
  stored inputs: {"namedArgs":{},"positionalArgs":[1.5]}
  valid JSON: True

float('nan'):
  status: SUCCESS
  stored inputs: {"namedArgs":{},"positionalArgs":[NaN]}
  valid JSON: False

REPRODUCES: workflow SUCCEEDED but its durable inputs column is not valid JSON
```

`JSON.parse('{"positionalArgs":[NaN]}')` in Node likewise throws
`Unexpected token 'N' ... is not valid JSON`.

## Expected

The portable format is documented as "straightforward use of JSON that all SDKs
can read and write", and as data that "can even be read and written from the
database without any DBOS code at all"
([Cross-Language Interaction](https://docs.dbos.dev/explanations/portable-workflows)).
Under that contract the stored input should be valid JSON — either by encoding
these values in a portable way or by rejecting them with a modeled error —
rather than being written as the non-JSON tokens `NaN` / `Infinity` /
`-Infinity` and reported as a successful workflow.

This appears to come from `dbos/_serialization.py`, where
`DBOSPortableJSONSerializer.serialize` calls `json.dumps(...)` without
`allow_nan=False`; Python's `json.dumps` then emits the non-standard `NaN` /
`Infinity` tokens for float infinities and NaN.
