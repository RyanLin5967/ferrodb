#!/usr/bin/env python3
"""E79b mutant runner: break one rule, prove its named test fails, restore."""
import subprocess, sys, os

REPO = "/Users/idide/wt/ferrodb-E79b-prompt-hash"
os.chdir(REPO)
ENV = dict(os.environ, PATH=os.path.expanduser("~/.cargo/bin") + ":" + os.environ["PATH"])

MUTANTS = [
 ("M1 PROMPT reserved as a keyword",
  "src/parser/scanner.rs",
  '            "MODEL" => TokenType::Model,',
  '            "MODEL" => TokenType::Model,\n            "PROMPT" => TokenType::Model,',
  ["cargo","test","--lib","parser::"],
  ["prompt_is_not_a_reserved_word","prompt_is_still_a_usable_column_and_table_name"]),

 ("M2 an omitted clause defaults to the empty prompt",
  "src/parser/parser.rs",
  "        } else {\n            None\n        };\n        self.end_of_agent_session_clauses()?;",
  "        } else {\n            Some(String::new())\n        };\n        self.end_of_agent_session_clauses()?;",
  ["cargo","test","--test","integration_prompt_clause"],
  ["an_omitted_clause_is_all_zero_and_an_empty_prompt_is_not"]),

 ("M3 an empty prompt special-cased back to the placeholder",
  "src/agent_sql/runtime.rs",
  "        let prompt_hash = prompt.map(prompt_digest).unwrap_or([0u8; 32]);",
  "        let prompt_hash = prompt.filter(|p| !p.is_empty()).map(prompt_digest).unwrap_or([0u8; 32]);",
  ["cargo","test","--test","integration_prompt_clause"],
  ["an_omitted_clause_is_all_zero_and_an_empty_prompt_is_not"]),

 ("M4 the prompt trimmed before hashing",
  "src/agent_sql/runtime.rs",
  "        let prompt_hash = prompt.map(prompt_digest).unwrap_or([0u8; 32]);",
  "        let prompt_hash = prompt.map(|p| prompt_digest(p.trim())).unwrap_or([0u8; 32]);",
  ["cargo","test","--test","integration_prompt_clause"],
  ["two_prompts_that_differ_produce_two_hashes"]),

 ("M5 RUN and PROMPT transposed on the way to the runtime",
  "src/agent_sql/dispatch.rs",
  "                    run_id: run_id.as_deref(),\n                    model,\n                    prompt: prompt.as_deref(),",
  "                    run_id: prompt.as_deref(),\n                    model,\n                    prompt: run_id.as_deref(),",
  ["cargo","test","--test","integration_prompt_clause"],
  ["the_run_id_and_the_prompt_are_not_transposed"]),

 ("M6 the prompt text kept beside the digest (recorded as the run id)",
  "src/agent_sql/runtime.rs",
  '        let run = run_id.unwrap_or("<unnamed>").to_string();',
  '        let run = prompt.map(|p| p.to_string()).unwrap_or_else(|| run_id.unwrap_or("<unnamed>").to_string());',
  ["cargo","test","--test","integration_prompt_clause"],
  ["the_prompt_text_never_reaches_the_durable_provenance_file"]),

 ("M7 the misplaced-clause error quotes the token before it",
  "src/parser/parser.rs",
  "        Err(Parser::error(\n            self.peek(),\n            format!(\n                \"{word} is repeated or out of order;",
  "        Err(Parser::error(\n            self.previous(),\n            format!(\n                \"{word} is repeated or out of order;",
  ["cargo","test","--lib","parser::parser::tests::a_repeated_or_misplaced_clause"],
  ["a_repeated_or_misplaced_clause_is_named_without_quoting_the_prompt"]),

 ("M8 the clause parsed and then dropped at dispatch",
  "src/agent_sql/dispatch.rs",
  "                    prompt: prompt.as_deref(),",
  "                    prompt: None,",
  ["cargo","test","--test","integration_prompt_clause","--test","integration_run_identity_feed","--test","integration_system_views"],
  ["a_prompt_declared_in_sql_becomes_the_runs_digest",
   "a_prompt_declared_over_sql_reaches_the_feed_as_a_digest",
   "ferro_runs_reports_the_digest_of_a_declared_prompt"]),
]

import os as _os
if _os.environ.get("ONLY"):
    keep=set(_os.environ["ONLY"].split(","))
    MUTANTS=[m for m in MUTANTS if m[0].split()[0] in keep]
out = open(_os.environ.get("OUT","scratchpad/E79b-mutants.txt"), "w")
def say(m):
    print(m); out.write(m + "\n"); out.flush()

# the tree must be clean before and after, or a mutant is being measured against the wrong tree
dirty = subprocess.run(["git","status","--porcelain"],capture_output=True,text=True).stdout.strip()
dirty = "\n".join(l for l in dirty.splitlines() if "scratchpad/" not in l)
if dirty:
    say("REFUSING: tree is dirty before the run:\n" + dirty); sys.exit(2)

ok = True
for name, path, old, new, cmd, expect in MUTANTS:
    src = open(path).read()
    if src.count(old) != 1:
        say(f"{name}: SKIPPED — anchor matched {src.count(old)} times in {path}"); ok = False; continue
    open(path,"w").write(src.replace(old,new))
    r = subprocess.run(cmd + ["--no-fail-fast"],
                       capture_output=True, text=True, env=ENV, timeout=1800)
    text = r.stdout + r.stderr
    open(path,"w").write(src)                     # restore before judging, always
    results = [l.strip() for l in text.splitlines() if l.startswith("test result:")]
    failed  = sorted({l.split()[1] for l in text.splitlines() if l.startswith("test ") and l.rstrip().endswith("FAILED")})
    build_err = "error[" in text or "error: could not compile" in text
    say(f"\n### {name}")
    say(f"    patch: {path}")
    say(f"    cmd:   {' '.join(cmd)}")
    if build_err:
        say("    RESULT: did not compile (mutant is not a valid program) — INVALID MUTANT"); ok = False; continue
    for l in results: say(f"    {l}")
    want_targets = cmd.count("--test") or 1
    if len(results) < want_targets:
        say(f"    *** {want_targets} target(s) named but {len(results)} ran — a target that did not RUN is not evidence ***"); ok = False; continue
    say(f"    failing tests: {failed if failed else 'NONE'}")
    missing = [e for e in expect if not any(f == e or f.endswith('::' + e) for f in failed)]
    if missing:
        say(f"    *** THE DETECTOR DID NOT FIRE for {missing} ***"); ok = False
    else:
        say("    detector fired for every named test")

dirty = subprocess.run(["git","status","--porcelain"],capture_output=True,text=True).stdout.strip()
dirty = "\n".join(l for l in dirty.splitlines() if "scratchpad/" not in l)
say("\ntree after restore: " + (dirty if dirty else "clean"))
if dirty: ok = False
say("\nALL DETECTORS FIRED" if ok else "\nSOMETHING DID NOT FIRE — see above")
out.close()
sys.exit(0 if ok else 1)
