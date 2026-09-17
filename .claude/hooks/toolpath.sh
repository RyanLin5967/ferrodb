# Sourced by every ferrodb hook. Resolves PY / GIT / TIMEOUT to binaries that actually RUN.
#
# ⛔ WHY THIS FILE EXISTS. 2026-09-17: a compaction landed, the handoff was not delivered, and the
# PreToolUse gate that is supposed to refuse every call until it IS read allowed everything. One
# cause, three symptoms. On this machine `/usr/bin/python3` and `/usr/bin/git` are Xcode shims, and
# the command-line-tools licence has never been accepted, so in a stripped hook environment they
# print a licence notice to STDERR, write NOTHING to stdout, and `/usr/bin/python3` exits 69:
#
#   sessionstart_handoff.sh  ->  `timeout 120 python3 ...` exits 69, assembler never ran, and the
#                                PREVIOUS compaction's .sessionstart_full.txt stayed on disk with
#                                its sentinel still matching -- a 4-hour-old handoff reading as current.
#   precompact_bank_state.sh ->  every `git` call returned empty, so branch, HEAD, ahead-of-origin
#                                and uncommitted were all BANKED BLANK, silently.
#   pretooluse_handoff_read.sh -> its payload parser is inline python3. No stdout means TRANSCRIPT=""
#                                which lands on the rung that says "no readable transcript -> MUST
#                                NOT BLOCK". The gate did not malfunction; it fell open, by design,
#                                on an input it could not read. That is the documented fail-open the
#                                standing rules keep naming, arriving through the interpreter itself.
#
# An interactive shell has homebrew ahead of /usr/bin, so all three work by hand and fail only in
# the hook -- which is why this survived a fire-check that was run by hand.
#
# PROBE, DO NOT ASSUME. Each candidate is EXECUTED and must produce the expected stdout; a binary
# that exists and refuses to run is exactly the case that caused this. If none works, the caller is
# told which tool is missing rather than continuing with an empty string.
_probe() {  # _probe <varname> <test-args...> -- candidates...
  local var="$1"; shift
  local -a test_args=(); while [ "$1" != "--" ]; do test_args+=("$1"); shift; done; shift
  local c
  for c in "$@"; do
    [ -x "$c" ] || continue
    if "$c" "${test_args[@]}" >/dev/null 2>&1; then printf -v "$var" '%s' "$c"; return 0; fi
  done
  printf -v "$var" '%s' ""; return 1
}

_probe PY -c 'import sys' -- /opt/homebrew/bin/python3 /usr/local/bin/python3 \
       "$HOME/.pyenv/shims/python3" "$(command -v python3 2>/dev/null)" /usr/bin/python3 \
  || echo "TOOLPATH: no working python3 -- hook degraded" >&2
_probe GIT --version -- /opt/homebrew/bin/git /usr/local/bin/git \
       "$(command -v git 2>/dev/null)" /usr/bin/git \
  || echo "TOOLPATH: no working git -- hook degraded" >&2
_probe TIMEOUT 1 true -- /opt/homebrew/bin/timeout /opt/homebrew/bin/gtimeout \
       /usr/local/bin/timeout "$(command -v timeout 2>/dev/null)" \
  || TIMEOUT=""   # absent is survivable: the commands here are already small and bounded by the hook

# Prepend the directories of the binaries that actually work, so the ~20 BARE `git` / `python3` /
# `timeout` calls in these hooks resolve to them too. Patching each call site individually would
# leave the next one added to silently pick the shim again -- which is how this bug survived: the
# banker had 20 bare `git` calls and every one of them returned empty into a file nobody re-read.
for _d in "$PY" "$GIT" "$TIMEOUT"; do
  [ -n "$_d" ] && case ":$PATH:" in *":${_d%/*}:"*) ;; *) PATH="${_d%/*}:$PATH" ;; esac
done
export PATH PY GIT TIMEOUT
