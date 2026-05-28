# Phase reports

This is where every phase report from `04_veer_testing_playbook.md` lives.

## Naming convention

```
P<NN>-<short-kebab-title>.md
```

Examples:
- `P00-setup-smoke.md`
- `P01-auth-login-signup-reset.md`
- `P18-blasts-tcpa-opt-in.md`
- `SUMMARY.md`  (final wrap-up after Phase 28)

`NN` is two-digit so the folder sorts in order. The short title matches the section anchor in the playbook.

## When you finish a phase

1. Copy `_template.md` to a new file using the naming convention above.
2. Fill it in. Keep it short — a clean phase can be 5 lines.
3. Commit on the `staging` branch.
4. Open a PR titled `Phase NN report: <title>` → `staging`.
5. Tag Brij. Wait for review. Merge.
6. Move to the next phase.

## When Cursor helps you

Tell Cursor:

> "Track my testing progress in `docs/Veers Tasks/phase-reports/`. The current phase is N. Use `_template.md` as the starting point for the new report."

Cursor will know to read prior reports for context (so it doesn't re-flag the same gap twice) and write the new one in the right place.

## File list as you go

Cursor and Brij can both `ls` this folder to see how far along you are. That's the whole point of keeping reports here instead of in random gists/Notion pages.
