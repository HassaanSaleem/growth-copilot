# Architecture

This document explains the design decisions behind growth-copilot in more depth than the
README. Each pattern here is one a production-grade LLM analytics orchestrator needs;
each is rebuilt with the LangGraph primitive that fits it best. File references are relative to
`src/growth_copilot/`.

## State design

The whole main graph flows through one flat `TypedDict` — `CopilotState` in
`domain/state.py` — with `total=False` so every stage reads and writes only the keys it
owns. There is no nested state object, no per-node private state class, no "context"
wrapper. Three decisions matter:

**Reducers make fan-out lossless.** `results` is annotated with a `merge_results`
reducer and `grounding`/`findings` with `append_list`. When `Send` fans a level of tasks
out in parallel, each `run_task` branch returns a single-entry
`{"results": {task_id: result}}` update; LangGraph folds all of them through the reducer
in the same superstep. Parallel writes never race and never overwrite — merging is a
property of the state schema, not of any coordination code.

**Task results use string keys.** `results` maps `str(task.id) -> result` even though
task ids are ints. Checkpoints serialize state through JSON, and JSON object keys are
strings — an int-keyed dict does not survive a save/load round-trip intact. Rather than
normalizing at every boundary, the state contract says string keys everywhere, and the
scheduler converts (`done = {int(k) for k in state["results"]}`) at the single place it
needs int ids back.

**Fan-out branches get a private input type.** `Send("run_task", ...)` does not pass the
shared state; it passes a `TaskInvocation` — the task dict plus a pre-sliced
`dep_results` containing only that task's declared dependencies. A task physically cannot
read results it did not declare an edge to, which keeps `depends_on` honest: the plan's
edges are the *only* channel for data flow.

## The schedule ⇄ run_task loop is Kahn's algorithm as supersteps

`graph/scheduler.py` contains a classical Kahn's algorithm (`build_batches`) used for
plan validation and display: compute in-degrees, repeatedly emit the zero-in-degree
level, decrement children, and if tasks remain, name the cycle members in the error.

Execution uses the same idea incrementally. `schedule` is a no-op barrier node; its
router `route_schedule` (`graph/nodes.py`) computes the ready set — tasks not yet in
`results` whose dependencies all are — and returns one `Send("run_task", ...)` per ready
task. Every `run_task` edge leads back to `schedule`, so the loop is:

```
schedule → [Send × ready tasks] → run_task (parallel) → schedule → ... → enrich
```

Each pass around the loop is one Kahn level; the barrier between passes is LangGraph's
own superstep semantics (all fan-out branches complete, reducers merge, then the router
runs again). When the ready set is empty the router returns `"enrich"` and execution
ends. There is no executor class, no futures bookkeeping, no queue — roughly 20 lines of
router replace all of it. Parallelism within a level is capped by `max_concurrency`
(`GROWTH_COPILOT_MAX_PARALLEL`, default 8) rather than by graph shape.

**Failures are data.** `run_task` catches every exception and returns
`{"status": "error", "execution_error": ...}` as that task's result. Siblings keep
running. A downstream task that `$ref`s a failed dependency fails in
`bind_dependencies` (`graph/binding.py`) with a `DependencyError` naming the failed
task — which is itself caught and becomes *that* task's error result. Failure propagates
along plan edges as ordinary data, the run always reaches `synthesize`, and the synthesis
prompt is explicitly instructed to acknowledge gaps instead of papering over them.

**Late binding.** Plans are written before any data exists, so args may contain
`{"$ref": <task_id>, "field": "segment_name"}` placeholders. `bind_dependencies`
resolves them recursively (through nested dicts and lists) against the pre-sliced
`dep_results` at execution time. Edge semantics live entirely in `binding.py`; the
scheduler moves data but never interprets it.

## Why enrich is deterministic

`enrich` is the "emergent analysis" stage: work the plan didn't ask for but the data
invites. When a run produced exactly two comparable successful results of the same tool —
two `segment_event_discovery` outputs or two `profile_segment` outputs, the classic
converted-vs-stalled cohort shape — it computes their deltas with
`warehouse.compare_discoveries` / `warehouse.compare_profiles` and appends a finding.

It is deliberately not an LLM call, for the same reason grounding isn't: the comparison
is arithmetic (lift deltas, share deltas), and arithmetic done by an LLM is a liability.
The copilot's division of labor is strict — the LLM decides *what* to compute and *what
it means*; code computes. The findings land in state as data and flow into the synthesis
prompt alongside the task results, so the LLM narrates a comparison it could not have
miscalculated.

## The bottleneck drill-down work queue

`graph/drilldown.py` handles the shape that breaks static graph frameworks: a recursive
cohort analysis whose tree isn't known until execution. Each cohort's funnel decides
whether and where to recurse, so no compile-time graph can enumerate the nodes. Hand-rolled, this shape is a pile of
worklist code; here it is a frontier/collect loop:

```
init → (route) ─┬→ [Send("expand_cohort", item) × frontier] → collect → (route)
                └→ report → END
```

`expand_cohort` runs the cohort's funnel, finds the worst step (largest absolute user
loss between consecutive steps), mines property-value blockers with `funnel_breakdown`,
and *proposes* child cohorts — it never spawns work itself. `collect` promotes only unseen
proposals to the next frontier. The tree grows breadth-first, one depth level per
superstep, with all cohorts at a level expanded in parallel.

Unbounded recursion is the failure mode of this shape, so expansion is bounded three
ways:

1. **Depth bound.** Items carry `depth`; children are only proposed while
   `depth < max_depth` (CLI `--depth`, default 3).
2. **Adaptive size threshold.** A child cohort must have
   `affected_users >= max(200, int(0.10 * parent_lost_users))` — the constants
   `MIN_COHORT_FLOOR = 200` and `ADAPTIVE_FRACTION = 0.10`. The floor stops the tree from
   chasing statistically meaningless slivers; the fraction makes the bar scale with the
   parent, so a 50,000-user loss doesn't fan out into hundreds of 200-user branches.
   A parent whose own lost-user count is below the floor proposes nothing at all.
3. **Signature dedup.** A cohort's identity is `filter_signature(filters)` —
   `json.dumps(filters, sort_keys=True)`, order-independent, so
   `{plan: free, device: android}` and `{device: android, plan: free}` are the same
   cohort. `proposals` is a dict keyed by signature with a merge reducer, which means
   *cross-branch dedup within a superstep is a property of the reducer*: two parallel
   branches proposing the same child collapse to one entry before `collect` ever runs.
   `collect` then filters against `seen` (also merge-reduced) so a cohort reachable via
   two different parent paths is expanded once, ever. Since reducers only merge and never
   clear, what counts as new is judged against `seen` rather than by emptying `proposals`.

Two smaller controls: at most `MAX_BLOCKERS_PER_NODE = 3` blockers are considered per
node, and a property already conditioned on along the branch is never re-proposed
(re-filtering on it would be a no-op cohort).

`report` renders the accumulated `nodes` list depth-first as markdown by rebuilding the
tree from each node's `parent` signature — the tree structure is recovered from data, not
tracked in control flow.

## Grounding is fail-open

`grounding.py` runs after planning and before execution. Every event name and property
reference in the plan (arg keys like `steps`, `events`, `start_event`,
`breakdown_property`, plus `filters` keys and values) is checked against the warehouse
metadata catalog — actual events, actual properties, actual property values.

- **Exact match** — untouched.
- **Near miss** (difflib `SequenceMatcher` ratio ≥ 0.9, case-insensitive) — corrected,
  and a record `{task_id, field, from, to, score}` is appended to state and streamed as a
  `grounding` event. This catches the canonical LLM failure: writing
  `file_uploade` for `file_uploaded`.
- **Below threshold** — left untouched and reported with `to: null`. Maybe the planner
  hallucinated; maybe the tool will legitimately handle it. Grounding improves plans, it
  never blocks them — the fail-open contract. If the value really is wrong, the task
  fails at execution, which the pipeline already treats as data (see above).

The 0.9 threshold is deliberately conservative: at 0.9 the correction is almost always a
morphological variant of the intended name; lower thresholds start "correcting" the LLM
into different real events, which is worse than failing. Corrections are visible in three
places — the live stream, the `grounding` state key, and the API's final SSE payload — so
a corrected plan is never silently different from the planned one.

## Offline / recipe mode

`llm.get_chat(stage)` returns `None` when `ANTHROPIC_API_KEY` is unset, and every caller
has a deterministic fallback — the copilot degrades, it doesn't crash:

- **Entry routing**: a state that already carries a `plan` (recipe mode) enters the graph
  at `ground`, skipping triage and planning entirely (`_route_entry` in
  `graph/build.py`). Recipes therefore never need an LLM key for planning.
- **triage** offline treats every input as `analyze` (no judgment available, so don't
  pretend to have any).
- **plan** offline raises immediately with an actionable message pointing at
  `growth-copilot recipe <name>` — planning is the one stage that genuinely requires
  judgment.
- **synthesize** offline renders `_deterministic_answer`: an honest, template-free
  readout of per-task `headline` fields, comparison highlights, and failures. No fake
  narrative.

A `Recipe` (`domain/tasks.py`) is a saved plan with `$param` placeholders resolved via
`string.Template.safe_substitute` at load time, validated straight into the same `Plan`
model the planner emits. This makes the full execution path — grounding, scheduling,
fan-out, binding, enrichment — exercisable offline, which is exactly what the test suite
and CI do. It is also the SME contract: a recipe is JSON, so editing an analysis is a
review-able diff, not a code deployment.

## Checkpointing semantics

The CLI compiles graphs with a `SqliteSaver` over a local SQLite file
(`GROWTH_COPILOT_CHECKPOINTS`, default `data/checkpoints.sqlite`). Every superstep is
checkpointed per thread. Two decisions ride on this:

**Deterministic thread ids, resumable-only reuse.** `thread_id_for(question)` is
`sha256(question.strip().lower())[:16]` (`graph/build.py`). Retrying a question whose
thread is still pending — suspended at an interrupt, or crashed mid-execution — lands on
the same checkpointed thread and resumes from the last completed superstep: planning
work already paid for is never repeated. A *completed* thread is deliberately never
reused: the CLI's `_resolve_thread` bumps to a fresh `-rN` suffix, because accumulating
reducers (`results`, `findings`, the drill-down `nodes`/`seen`) would otherwise merge a
finished run's state into the retry. Recipe runs get `recipe-{name}-{params-hash}`
threads and drill-down runs get `drilldown-{steps}-{days}-{depth}`, with the same semantics.
Passing `--thread` overrides the hash entirely.

**Durable interrupts.** When triage decides it needs clarification, it commits that
verdict to state and routes to a dedicated `clarify` node whose *first statement* is
`interrupt()` — on resume LangGraph replays the interrupted node from its start, so the
interrupt must not sit behind an LLM call whose replayed verdict could flip and discard
the user's answer. The pause is persisted in the checkpoint rather than in process
memory. The CLI loop detects `__interrupt__`, prompts the user, and resumes with
`Command(resume=answer)` on the same thread — even from a later process, since the
deterministic thread id plus the checkpoint carry everything needed. The API compiles its
graphs without a checkpointer (it is a stateless local demo service), so it surfaces an
`interrupt` SSE event and defers resumption to the CLI. Clarifications are appended to
state and folded into the question, so the planner sees the full exchange.

The checkpoint is also the post-hoc audit trail: `graph.get_state(config)` returns the
final state — plan, grounding corrections, per-task results, findings, answer — for any
thread id, long after the run finished.

## The warehouse seam

Everything the graph asks of a warehouse goes through four members — `get_connection`,
`execute_tool`, `metadata_catalog`, `exported_segment_names` — spelled out as the
`WarehouseRepository` Protocol in `warehouse/repository.py`. The module-level DuckDB
implementation satisfies it as-is (modules are valid protocol implementers), which is the
roadmap's "one adapter away" claim made checkable: a client-server warehouse backend means
implementing those four members, and nothing in `graph/nodes.py` changes, because the graph
never sees SQL.
