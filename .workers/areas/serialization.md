---
key: serialization
title: Serialization
description: Errors and inputs keep their meaning through durable storage — failed workflows preserve actionable error identity, and portable JSON inputs arrive matching their declared types.
order: 80
---

# Serialization

What this area covers: what survives the trip through the system database.
DBOS promises that a failed workflow's durable record preserves the
application error's type and message (not a serializer artifact), and that
workflows configured with the portable JSON serializer receive arguments
compatible with their Python type hints — datetimes normalized
deterministically, invalid values rejected with modeled errors rather than
silently accepted.

Boundaries:
- In scope: default and portable serializer error fidelity, retry/recovery
  error records, portable input type coercion across scheduled, queued,
  recovered, and directly inserted rows.
- Out of scope until a promise names them: custom third-party serializers,
  cross-language portability.

Evidence lineage: legacy hunt corpora in `areas/serialization-error-fidelity.md`
(rungs 000–005; open candidates on portable error metadata, rungs 003–004) and
`areas/portable-input-type-fidelity.md` (rung 001).
