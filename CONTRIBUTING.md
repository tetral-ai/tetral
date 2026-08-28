# Contributing

Contributions of any size and any authorship — human, AI, or both — are
welcome. Contributions nobody understands are not. A 300k-line change with a
clear paper trail can merge; a 3-line change nobody can explain cannot.

Read [AGENTS.md](AGENTS.md) first for the repository map, build and test
commands, and where each service's contract lives. What follows is the
pre-PR self-check, and it is tool-neutral: run it by hand, or have whatever
coding agent you use run it. Claude Code also exposes it as `/pr-selfcheck`.

This procedure extracts the evidence of understanding. Run it before opening a
PR and paste the filled answer sheet into the PR description. **If you cannot
fill a field truthfully, you are not ready to submit** — resolve the gap first,
do not paper over it. Reviewers spot-check the sheet against the diff; an
unexplained load-bearing change is grounds for rejection without further
review. Polish is the contributor's cost; clarity is everyone's gain.

## Local verification

Run `make test` while iterating; it needs no Docker, PostgreSQL, or MinIO. Run
`make test-affected` before submitting a focused change. The runner prints the
exact selection and starts only required disposable dependencies; uncertain
ownership expands to the full profile. `make test-full` is the complete local
pull-request gate and may take materially longer. Native `go test` and
`bun test` commands remain available for a single owning package.

## During the work, not after

Keep an implementation-notes file while you work. Whenever reality forces you
off your plan — an edge case, a surprising dependency, a rejected approach —
log it under a `Deviations` heading and keep going. The answer sheet below is
distilled from these notes; a sheet reconstructed from memory after the fact
reads differently from one grown during the work, and reviewers can tell.

## The five steps

1. **Inventory.** Read your own full diff (`git diff main...HEAD`). Classify
   every hunk: behavior / test / doc / mechanical. If you cannot say what a
   hunk is for, stop here.
2. **Interrogate the load-bearing hunks.** For each behavior hunk, answer in
   writing:
   - What breaks or is missing without this change?
   - Why this shape — why here, why this way, what alternative did you reject?
   - What could it break? Who calls this code; which invariant is nearby?
   For the third question, open the touched service's `README.md`, find its
   invariants (the *Seams* and *States & lifecycle* sections), quote the one
   your change lives next to, and state why it still holds.
3. **Docs follow behavior.** For each behavior change, name the owning doc
   (the touched service's README section) and either point at your doc delta
   or state in one line why no doc is affected.
4. **Tests prove claims.** Every behavioral claim in your sheet names the test
   that pins it. Run the focused suites and `make test-affected`; record the
   counts and any fail-closed expansion to Full.
5. **Fill the answer sheet** below and paste it into the PR description.

## The answer sheet

Fixed headings, fixed order. Write `none` under an axis that does not apply —
do not delete the heading. No free-form walls of prose: short entries under
the right heading.

```markdown
## Intent
<one or two sentences: the problem and the outcome>

## Information flow
- before: <who sent what to whom>
- after: <what moves differently now>

## Life cycle
- <object whose states/timing changed>: <the change>

## Ownership
- <fact or resource>: <owner before> → <owner after>

## Invariants
- "<verbatim quote>" (<which service README>) — <why your change preserves it>

## Deviations
- <what the work forced you to discover or change, from your notes; `none`
  on a large diff is itself a review signal>

## Docs
- <owning service README>: updated | not affected because <one line>

## Tests
- <claim> → <test name / file>, <pass counts>
```
