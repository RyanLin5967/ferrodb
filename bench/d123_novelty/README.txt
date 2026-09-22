⛔ THIS DIRECTORY CONTAINS NO RESULT. It holds one TRUNCATED run, kept as evidence of a failure
   shape, not as a measurement. The novelty arm's real output is bench/d123_final/40_novelty.txt.

WHAT HAPPENED. The lead armed mode_novelty while this agent was rate-limited. Their wrapper echoed
a `stamp:` line from its own rebuild moment — `build at dfbbd3683ac2 +DIRTY` — and they killed the
run on it (`Terminated: 15`). But the RUN'S OWN HEADER says `build at 8665941de637` with no +DIRTY:
the commit had landed 27 seconds before the lock was acquired, and the binary that actually
executed was the clean one. A good measurement was killed to avoid a contamination that had already
stopped existing.

⭐ THE REUSABLE LESSON, AND IT CUTS BOTH WAYS:
   * The artifact's OWN header is the authority. A wrapper's pre-run echo is a snapshot of a moment
     that may have passed by the time the binary is exec'd.
   * A truncated run is INDISTINGUISHABLE AT A GLANCE FROM A COMPLETED ONE. This file has a correct
     provenance banner, the mode title, and the k=8 preamble — everything a skim would check — and
     then simply stops. The check that catches it is the ABSENCE of the `# harness_exit=` sentinel
     the run scripts append, and absence is precisely what nobody looks for.
   * "The binary was already built at <sha>" is not checkable from the binary's existence. Run it
     with a bad argument and read the list of modes it admits; that is what established these
     builds never contained mode_novelty at all.
