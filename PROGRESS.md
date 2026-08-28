# E79b — a prompt to hash

**Done**
- `PROMPT '<text>'` parses as an optional trailing clause of `BEGIN AGENT SESSION`, matched by
  lexeme (soft keyword) so `prompt` stays a usable identifier. AST/bound stmt carry `Option<String>`.
- `AgentRuntime::begin_session_as(RunIdentity, parent)` is the single body; it hashes the prompt with
  `prompt_digest` into `RunEntity::prompt_hash`. No clause => `[0u8; 32]`, which is NOT
  `prompt_digest("")`.
- `begin_session` / `begin_session_with_model` delegate; dispatch passes the parsed prompt through.

**Doing now**
- Named tests + mutants for each rule; extending
  `tests/integration_run_identity_feed.rs` to the SQL path and correcting the pinned all-zero
  assertion in `tests/integration_system_views.rs:223`.

**Next action**
- Write `tests/integration_prompt_clause.rs` (end-to-end + the durable-store privacy canary).
