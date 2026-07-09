# Run: <WP-id>

## Summary

| Field | Value |
|---|---|
| Work item | <WP-id> |
| Area | <area-id> |
| Rung | <rung-id> |
| Target commit | <commit> |
| Harness commit | <commit> |
| WIO project | DBOS Workload Fresh / kn7a3jjm0frn1qgwpms30amdas88ztwy |
| WIO batch | <batch-id> |
| WIO run IDs | <run-id-list> |
| Build profile | default |
| Status | queued |

## Replay

Cloud command:

```bash
wio simulate create kn7a3jjm0frn1qgwpms30amdas88ztwy --branch main --command "<command>" --workload-path "<path>" --depth <n> --timeout <seconds> --mem <MiB>
```

Workload command:

```bash
<command>
```

Local reproduction, if used:

```bash
<local reproduction command or "not used">
```

## Evidence

- Seeds/cases:
- WIO logs/artifacts:
- Invariant result:
- Finding:
- Local reproduction result, if any:

## Classification

Use one: `green`, `finding`, `low_signal`, `blocked_setup`,
`blocked_workload`, `blocked_build_regression`, `stale`.

## Notes
