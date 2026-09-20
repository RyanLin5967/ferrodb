D54 CERTIFICATION — the shared read path, with the AS OF fix

  RUNNER.txt / suite.log / suite.go.log     head=79004da  <- WHAT THE LANDING RESTS ON

CERTIFIED GREEN:

  D54-asof-fix: mode=per-target rc=0 passed=2100 failed=0 build_errors=0 head=79004da
  D54-asof-fix: go rc=0 passed=97 failed=0

tools/certify-head.sh: OK -- suite head=79004da == landing 79004da.

⛔ READ THIS BEFORE TRUSTING THE COUNT. An EARLIER run of this same suite went green -- rc=0
passed=2099 failed=0 head=bb583dd -- WITH A WRONG-ROWS BUG PRESENT. `SELECT ... AS OF BRANCH x`
inside an agent session read the session's own branch. 2099 tests could not see it because
almost every test in this repo drives executor::run through its own Db::exec fixture, and the
new read path (try_run_read) sits ABOVE run, reached only from pgwire::extended.

So this green means "2100 tests pass", which is not the same as "the change is right". What
makes it evidence about THIS change is tests/d54_as_of_in_session.rs, which goes through
pgwire::extended and FAILED on the previous commit with the message it was written to produce.
The count is the floor, not the argument.
