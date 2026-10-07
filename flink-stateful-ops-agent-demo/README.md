# Flink Stateful Ops Agent Demo

An operations agent, itself a Flink job built with [Apache Flink Agents](https://github.com/apache/flink-agents),
watches other Flink jobs and follows a **stateful runbook**: the Flink 2.2
[Balanced Tasks Scheduling](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/deployment/tasks-scheduling/balanced_tasks_scheduling/)
guide. It waits for evidence, runs an experiment, checks the result against a baseline, and hands off to a
person when the fix doesn't hold. Every step it takes is kept in per-job Flink state, so the agent survives a
crash without forgetting anything or repeating a side effect.

This is the demo for the re:Invent 2026 talk OPN303 (slide 11). It is shown as a muted 7-minute recording,
narrated live from [`docs/demo-script.md`](docs/demo-script.md).

## Why the runbook needs memory
Each line of the runbook depends on what already happened:

| The runbook says | So the agent must |
|---|---|
| "If you are seeing these bottlenecks…" | see the skew persist across several windows, not react to one snapshot |
| "…may degrade performance if not needed" | record a baseline first, then keep or revert the change |
| "Increase `slot.request.max-interval` by 50 ms each time" | remember the current value and how many times it has tried |
| "If still unbalanced, report it in FLINK-38715" | escalate with the full history of attempts as evidence |

A request-driven agent would need its own database, scheduler, queue and partitioner for this. Here, each
job's memory is Flink keyed state, checkpointed with the stream.

## What happens in the demo
| # | Scene | What you see | Real or simulated |
|---|---|---|---|
| 1 | The runbook | The Flink docs, with the four stateful phrases highlighted | — |
| 2 | The agent is a Flink job | Its job graph: Kafka → 15 s windows → persistence filter → agent at parallelism 4. A funnel: ~3,500 metric samples → <30 windows → 5 LLM calls | real |
| 3 | The page | `ClickstreamEnrichment` is backpressured: one TaskManager runs 4 tasks at 100% CPU, another runs 2. The agent waits for 3 skewed windows, then writes a baseline into memory | real |
| 4 | The fix, verified | The LLM picks `APPLY_TASKS`. The agent redeploys the cluster with TASKS balancing (savepoint → new config → restore), tasks even out to 3/3/3, and after a cooldown it verifies: ~7.5k → 9.5k records/s, backpressure gone → **KEEP**. Same 3 machines | real |
| 5 | When the fix doesn't hold | Fleet job `payments-sessionizer`: TASKS is already on, but the skew returns after failovers. The agent raises the interval 20 → 70 → 120 → 170 ms, then a guardrail in code stops it and it **ESCALATES** with a report built from memory | **simulated** (labeled on screen) |
| 6 | Break the agent | `kill -9` on the agent's TaskManager while a redeploy is in flight. Flink restores the agent from its checkpoint; the agent finds its in-flight request instead of resending it (**redeploys: 1**), and its memory is intact | real |
| 7 | Close | KEPT 1 · REVERTED 0 · ESCALATED 1 | — |

The fleet job is simulated because post-failover imbalance doesn't reproduce on demand on a laptop.

## How it works
```
platform-sim (localhost:8090): stand-in for metrics, a deployment API, a ticket system, plus the dashboard
  │  every 3 s: one metric sample per job
  │    real:      ClickstreamEnrichment on the target cluster (Flink REST, localhost:8081)
  │    simulated: 21 fleet jobs (20 healthy + payments-sessionizer)
  ▼
Kafka ops_metrics
  ▼
Agent Flink cluster (localhost:8082), job "stateful-ops-agent"
  parse → keyBy(job) → 15 s windows → persistence filter → OpsAgent (keyed by job, parallelism 4)
                                                               │ LLM: Ollama qwen3:8b, structured output
                                                               │ side effects: REST calls to platform-sim
  ▼                                                            ▼ (redeploy / escalate, durable)
Kafka ops_records → dashboard
```

**The agent** (`agent-job/`)
- **Memory:** per-job short-term memory (Flink keyed state): the open incident, its baseline, every attempt
  with before/after measurements, the cooldown, and past incidents.
- **Runbook as a Skill:** `skills/balanced-task-scheduling/SKILL.md` cites the Flink docs and has a decision
  table. It is loaded into every decision prompt, because an ops agent shouldn't choose whether to read the runbook.
- **Structured decisions:** the LLM returns `runbook_step`, `action` and `rationale` (`output_schema`).
- **Guardrails in code:** `allowed_actions()` limits which actions the LLM may pick in each state; after 3
  interval increases, escalation is forced by code, not by the prompt; a 30 s cooldown precedes every verify.
- **Durable side effects:** the redeploy and the escalation run through `ctx.durable_execute_async` with a
  stable request ID and a reconciler. After a crash the reconciler asks the platform about that request ID
  and waits for it, instead of sending it again.
- **Stream before reasoning:** windows and the persistence filter drop healthy windows, so the model only
  sees problems that persist.

**The target** (`target-cluster/`, `target-jobs/`)
- A Docker Flink 2.2.1 session cluster: 1 JobManager, 3 TaskManagers × 2 slots, `TM_CPUS` each (default 0.5).
- `ClickstreamEnrichment`: Enrich (p=6) → keyBy → SessionScore (p=3), both CPU-heavy, fed 9,500 events/s.
  The default scheduler (`NONE`) places it 4/3/2, which can't keep up; `TASKS` places it 3/3/3, which can.
- Both settings are cluster-level, so the fix is a redeploy from a savepoint. In production this would be a
  Kubernetes operator spec change.

## Run it
Requirements: macOS or Linux, Docker, [Ollama](https://ollama.com), Python 3.10–3.12, JDK 17, Maven, git,
and 16 GB of RAM or more. Tested on an M3 Pro MacBook (36 GB).

Start Docker and Ollama, then from this directory:
```bash
bin/demo.sh setup            # once: Flink 2.2.1, venv, flink-agents (main + #1161) built from source, job jar, model (~7 GB of downloads)
bin/demo.sh up               # Kafka, target cluster (skewed), agent cluster, model, platform-sim
bin/demo.sh agent            # submit the agent; watch the dashboard at http://localhost:8090
bin/demo.sh fleet-incident   # after the KEEP: start the simulated fleet incident (scene 5)
bin/demo.sh reset            # back to the start, before each run
bin/demo.sh agent && bin/demo.sh kill-agent-tm --wait    # scene 6 (after a reset): kill the agent mid-redeploy
bin/demo.sh down --all       # stop everything and unload the model
```

| URL | What |
|---|---|
| http://localhost:8090 | Dashboard: press `1` for the real job, `2` for the fleet |
| http://localhost:8081 | Target cluster Flink UI (`ClickstreamEnrichment`) |
| http://localhost:8082 | Agent cluster Flink UI (`stateful-ops-agent`) |

`bin/demo.sh status` shows what's running. Settings (environment variables): `TM_CPUS`, `TARGET_RATE`,
`WINDOW_SECONDS`, `COOLDOWN_SECONDS`, `OLLAMA_MODEL`; see the header of `bin/demo.sh`.

**Versions:** Flink 2.2.1 and flink-agents built from `main` plus
[apache/flink-agents#1161](https://github.com/apache/flink-agents/pull/1161). The released 0.3.1 can't show
"no double redeploy" after a crash; #1161 fixes that. Use the first release that contains it once available.

## Docs
| File | For |
|---|---|
| [`docs/demo-script.md`](docs/demo-script.md) | The talk: timing, narration and on-screen shot list for each scene, Q&A prep, deck changes |
| [`docs/recording-runbook.md`](docs/recording-runbook.md) | Recording the video: commands, what to look at and when, measured timings |
| [`docs/plan.md`](docs/plan.md) | Design notes: decisions, architecture, and what the spikes found |

## Layout
```
agent-job/        the agent: Flink job, pipeline, agent, runbook logic, platform calls
skills/           the runbook as an Agent Skill
platform-sim/     collector, deployment and escalation APIs, simulated fleet, dashboard
target-cluster/   the target Docker cluster and its control script
target-jobs/      ClickstreamEnrichment (Java)
bin/              demo.sh and setup scripts
spikes/           early experiments behind docs/plan.md
```
