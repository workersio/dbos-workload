# [closed] replacement_children cleanup semantics for forked workflows

Status: `closed`

Disposition: filed upstream as `dbos-inc/dbos-transact-py#735` and closed as
intended behavior/docs follow-up. Keep as contract history; do not file a
duplicate.

## Summary

When `fork_workflow(..., replacement_children=...)` is used, the forked parent
uses replacement child outputs, but those replacement workflows are not visible
as children of the forked parent and are not removed by
`delete_workflow(..., delete_children=True)`.

The filed issue asked whether replacement children become part of the forked
parent lifecycle graph or are execution-only substitutions that callers must
track and clean up separately.

## Upstream Status

- Issue: https://github.com/dbos-inc/dbos-transact-py/issues/735
- State checked on 2026-06-25: `CLOSED`
- Maintainer response summary: replacement children are intentionally not
  adopted by the forked parent; the same replacements can be passed into
  multiple forks, and docs should clarify the behavior.

## Local Evidence

- Original local draft:
  `/Users/viswa/code/workers/dbos-replacement-children-github-issue-draft-20260622.md`
- Map promoted finding:
  `.workers/map.md`
- Area: `.workers/areas/lifecycle-fork-state.md`

## Local Disposition

Closed as intended behavior with documentation clarification. Keep this local
note to prevent duplicate issue filing and to guide future lifecycle workloads.
