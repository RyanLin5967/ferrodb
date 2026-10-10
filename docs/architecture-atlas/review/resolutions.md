# Completed-model review resolutions

These are corrections to the explanatory artifact, not application-code fixes.

| Review finding | Resolution in the model |
|---|---|
| Physical author stamps can occur during row application, not only after commit | Merge notes distinguish physical version stamps from post-commit logical authorship/history; separate provenance durability remains explicit. |
| Target box includes heap plus RAM history/counters | Renamed to “Current target state.” |
| Combined simulation/cherry-pick box does not always use the evaluator | Edge now says “reuse core paths.” |
| Ordinary checksum fields are not verified COW-style CRCs | Ordinary labels say “checksum field”; details distinguish the actual formats and validation. |
| MVCC helper had an incorrect name | Corrected to `resolve_visibility`. |
| Direct primary-key read path was not drawn | Added executor-to-primary-index edge, independently of secondary lookup. |
| Ordinary DML also writes WAL | System caption and dedicated ordinary-WAL flow make this explicit. |
| Reaped slots are not unconditionally reusable | Lifecycle node and detail require descendant-safe reuse after generation fencing. |
| Fork illustration reversed leaf order visually | Parent leaf order now remains 12 then 19; child’s shared-page arrow is routed around the leaf row. |
| Page fields versus page-to-page links could be confused | Header-to-payload arrows say “within page”; child links are labeled separately, with an explicit note that the child has its own header. |
| Ownership equation could be vague | COW engine detail gives `owns arena && birth >= max(own fork, latest live child fork)`. |
| Lease time is not identical in standalone and cluster components | Standalone wall time and replicated cluster time are distinguished. |
| WAL contains uncommitted records too | Component label says “Row records + transaction markers”; CDC’s commit filtering is separate. |
| Startup lock could be confused with transaction/page synchronization | Named `DbLock` with process-exclusion explanation. |
| Edge labels collided with adjacent boxes | Shortened horizontal labels (`then`, `RID`, `fresh RAM`); final browser collision audit checks the rendered geometry. |
| Full print caused a sparse table spill page | Added compact visual print mode while preserving selectable explanations and optional full-detail printing in HTML. |

All four content reviewers accepted their corrected scopes; see their review files for the source evidence and acceptance addenda. Browser validation and final completion status are recorded separately.
