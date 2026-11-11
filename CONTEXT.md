# Domain Context

This project reviews QQ group join requests using Bilibili account checks and
per-group review pipelines. A supported deployment runs one Bot process.

## Terms

- **Join request**: one OneBot group-add event, identified for action deduplication
  by Bot ID, group ID, subtype, and a nonempty request flag.
- **Missed request recovery**: startup/reconnect and periodic discovery of explicitly unchecked
  SnowLuma or NapCat join requests for enabled groups, using each implementation's
  verified response contract. It runs current admission rules and
  shares durable request deduplication with live events; it does not resolve
  uncertain external action outcomes or infer approval from list absence.
- **Check result**: evidence obtained from a Bilibili or QQ query. `passed`,
  `failed`, `inaccessible`, and `not_required` describe the check, not the final
  admission decision. An incomplete or unavailable query cannot establish a miss.
- **Condition**: a configured rule evaluated as T (`true`), F (`false`), or
  I (`unknown`). Missing evidence and failed identity lookups remain unknown.
- **Review stage**: a named v2 condition aggregation or v3 safe AST expression, and one branch
  for each T/F/I result. A branch has one flow action and optional side effects.
- **Flow action**: `next`, `approve`, `reject`, or `ignore`. `next` can select a
  configured target stage. A reached cycle or missing target is an error.
- **Side effect**: an ordered `notify_admin` or `log_warn` action emitted by a
  stage. The stage trace is available before its side effects run.
- **Review decision**: the pipeline's intended admission result and reasons.
  This is separate from the outcome of a OneBot action.
- **Action status**: `pending` is durable intent whose result is unconfirmed;
  `applied` is confirmed execution; `failed` is an explicit action rejection;
  `not_required` means the audit does not require an external approval action.
- **Binding**: a recorded QQ/Bilibili identity relationship used by `no_conflict`
  and `no_identity_change`. It does not prove initial account control.
- **Account input**: an extracted join-request answer classified as explicit or
  numeric UID, or nickname. UID lookup and nickname search never cross-fallback;
  malformed marked UIDs fail local validation. Inaccessible APIs remain unknown.
- **Rejection explanation**: confirmed facts, requirements, and a next action,
  with configured text kept as a supplement. Unknown-state rejections explain
  incomplete verification; true-state rejections explain the configured policy.
  A failed medal check retains its source detail alongside target UIDs and the
  minimum level; unavailable medal data remains unknown.
- **Identity decision lock**: process-local serialization of the QQ and Bilibili
  UID from binding lookup through decision finalization. Manual resolution and
  binding removal share the same lock manager.

## Implementation boundaries

`src/gatekeeper/` owns framework-independent models, condition calculation,
pipeline traversal, safe Kleene expressions, formatting, input parsing and locks.
Its imports do not register handlers or create runtime files.

`app/` owns standalone application services, versioned SQLite configuration,
records, request-specific recovery, Bilibili adapters, OneBot transport and one
FastAPI lifecycle for administration, reverse WS and the ownership gateway.
`bot.py` starts this runtime without NoneBot. `legacy_bot.py` explicitly loads the
optional `app/nonebot_adapter.py` event/action bridge into the same services.
The archived `src/plugins/group_request_manager/` implementation is regression
material and must not open a migrated v4 database. See
[ADR 0003](docs/adr/0003-standalone-runtime-sql-config.md).

SQLite v4 preserves all v3 business rows and imports the effective legacy
configuration once. Subsequent edits use CAS-protected immutable revisions.
Business rollback preserves the live credential bundle. Timeout isolates only
the affected identities; exact request evidence is required for recovery or the
single permitted external replay. Membership alone is insufficient.

Optional ownership evidence does not bypass admission rules. Applicant QR
credentials are ephemeral and isolated from operator credentials. Patrol is
disabled by default, uses an explicit qualification expression, confirmed
group-specific identity and fresh member/evidence checks; unknown observations
never independently authorize a removal.

Current operator-facing behavior is documented in the
[review rules](docs/review-requirements.md),
[configuration sources](docs/configuration.md), and
[rejection feedback](docs/rejection-feedback.md). Historical plans are separately
archived. Current implementation and validation boundaries are recorded in
[the implementation matrix](docs/v3-implementation.md).
