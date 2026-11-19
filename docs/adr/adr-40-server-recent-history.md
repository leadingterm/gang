# ADR-40: The server owns the dashboard's recent history

Status: Accepted. Supersedes ADR-25's "no server-side phase history"; amends ADR-27 (a watched pass always sends a delta). Amended by [ADR-55](adr-55-cache-facts-at-their-owners.md) for farm-wide cache traffic and evictions.

## Context
- The dashboard accumulated its charts in the browser (ADR-25): `Done / min` showed `–` for the first seconds, the spark, the control-pass strip and the worker lanes started empty, and two viewers of one farm saw different numbers depending on when they opened the page.
- Operators read the dashboard right after opening it, often during an incident; a figure that depends on how long the page has been open is wrong when it matters.
- The control pass already sees every number the charts need, once per pass, on the injected clock.

## Decision
- **One bounded window in the state.** `State.recent` holds one sample per `[dashboard] history_step` (10 s) over `history_window` (15 min): completions, executing and queued actions at the latest pass, completed and skipped passes. Each worker carries `phases`, its phase spans within the window. Both are in every snapshot (`GET /v1/state`, first stream frame).
- **One owner of the rules.** `gang_wire::Recent` (`sample_at`, `increment`, `apply`) is used by the builder, the diff and the client mirror; `gang_state::Builder` counts each pass into the sample of the pass's wall time.
- **Increments, not copies.** A delta carries the samples from the first one the pass changed.
- **In memory, honest about gaps.** A restart starts the window empty; the UI hatches steps before the first sample as not recorded and never shows them as zero. A controller restart counts the new completion counter; completions between the last pass and the restart are lost.
- **The browser derives nothing over time.** Every chart and rate is a pure function of the latest state.

## Consequences
- Every watched pass changes the step in progress, so `seq` advances per pass while someone watches (ADR-27's "unchanged passes send no delta" no longer holds); the delta is one sample.
- The state costs O(window / step + worker phase changes in the window) more memory, fixed by configuration.
- A new viewer and an old one show the same charts.

## Rejected alternatives
- **Browser-side accumulation (ADR-25):** depends on page age and differs between viewers.
- **Persisting the window in the database:** a second store for a view; a restart is rare and shown as not recorded.
- **A separate history endpoint:** two requests and a race between them; the snapshot already is the consistent view.
- **Per-pass samples:** unbounded by configuration and noisier than per-step aggregation.
