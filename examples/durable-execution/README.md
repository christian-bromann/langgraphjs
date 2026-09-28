# Can LangGraph.js run durable execution?

Thesis under test:

> LangGraph could power durable execution workloads the way Temporal does. Program state can live in delta channels, and each function call or stack frame can run as its own node, so a crash resumes from that point in time with the same state.

Short answer: **a checkpointed graph can resume finished steps. It cannot, as it exists today, host general durable code.** Delta channels compress reducer state. They are not a program counter. Putting every call in its own node is a real technique, and it works at superstep boundaries, but the product around that technique — queues, leases, durable timers, deterministic replay, and code versioning — is most of what durable-execution companies sell.

The proof is `src/approaches.test.ts`. It crashes workflows on purpose and counts side effects. Run it with `pnpm --filter @examples/durable-execution test`. The kill cases are a second process that calls `process.exit`, then a new process that loads the same checkpoint file.

| Case | What happened |
| --- | --- |
| Graph node throws, then a new process calls `invoke(null)` | `charge` and `reserve` once, `ship` twice, `notify` once |
| `process.exit` inside a later graph node, `durability: "sync"` | Previous nodes stay done. Resume with `invoke(null)` |
| Same kill, `durability: "exit"` | Nothing to resume. A fresh `invoke` repeats `charge` and `reserve` |
| `task()` throws, then a new process calls `invoke(null)` | Finished tasks are skipped. Lines between tasks run again |
| `process.exit` on the first line of the next `task()`, `durability: "sync"` | The task that just returned is **not** durable. `charge` runs twice. `invoke(null)` still finds the thread |
| Same, but kill one task later | `charge` once, `reserve` twice. One scheduling turn was enough for the older write to land, not the latest one |
| Read a `task()` result while `putWrites` is artificially slow | `writesStarted` is still 0 at the `await` |
| 20 steps, 1,500-character payload | 22 checkpoints either way. 362,120 bytes with a full array channel, 46,833 with a delta channel |
| Delete an earlier `pay()` step and resume | `pay("order-2")` returns `receipt:order-1` and the function does not run |
| Subgraph inner node throws, `durability: "sync"` | Outer `charge` once, inner `reserve` once, inner `ship` twice, with or without `checkpointer: true` |

## What durable execution actually is

A durable execution engine makes an ordinary-looking function survive process death. The function can sleep for a week, call an HTTP API, or wait for a person, and if the machine running it disappears, another machine continues **after the last completed step**. Completed side effects are not repeated. The step that was in flight might run again, so that step has to be idempotent.

That is a different claim from "we saved a checkpoint."

### How the engines do it

| Engine | What is recorded | How a crash resumes | Side-effect rule |
| --- | --- | --- | --- |
| [Temporal](https://docs.temporal.io/workflows) (successor of Uber Cadence) | Append-only **event history** of commands: schedule activity, timer, child workflow, signal | Worker re-executes workflow code from the start. Commands are matched against history. Cached activity results are returned. No match means the command is new | Workflow code must be deterministic. Activities are at-least-once and must be idempotent |
| [Restate](https://docs.restate.dev/guides/request-lifecycle) | Per-invocation **journal**. Handlers suspend at the next context call and the server stores the entries | Handler restarts and each journaled call returns the stored result until execution catches up | Journaled steps are not re-executed. Inter-handler calls go through the server so the callee is invoked once |
| [DBOS](https://docs.dbos.dev/python/tutorials/workflow-tutorial) | Step checkpoints in **Postgres** | On startup the library finds incomplete workflows and replays them, skipping completed steps | Steps are at-least-once. Workflow code must take the same sequence of steps |
| [Cloudflare Workflows](https://developers.cloudflare.com/workflows/) | Step results in Durable Object storage, persisted **before** the next step | Replay from the last completed step | Code outside `step.do` must be deterministic. A failed step may run more than once |
| Inngest, Trigger.dev | Step / event logs behind a hosted runner | The platform re-invokes the function; finished steps return cached results | Same shape as Cloudflare: wrap the side effect, keep the glue deterministic |
| Azure Durable Functions, AWS Step Functions | Orchestration history or a state-machine definition | Provider replays or restarts the state machine | Activities / tasks are retried; orchestrators must not do I/O themselves |

Two families show up everywhere:

1. **Replay.** The workflow function runs again from the top. A journal makes finished steps return instantly. Temporal, Restate, DBOS, and LangGraph's functional API all do this.
2. **Explicit state machine.** The program is a graph of states. The engine stores the current state and runs the next state. Step Functions, and LangGraph's Graph API, do this. Temporal's own pitch is that they replaced hand-written state machines with replay ([their writeup](https://temporal.io/blog/temporal-replaces-state-machines-for-distributed-applications)).

Both families still need the same operational machinery, which is not part of the programming model:

- A **store** that survives the worker (Postgres, Cassandra, a replicated log).
- A **queue and a lease**. Someone has to notice the worker died. Temporal does this with workflow tasks and heartbeats. A library that only writes checkpoints does not.
- **Durable timers**. `setTimeout` dies with the process. The timer has to be a row that a scheduler fires later.
- **At-least-once activities with idempotency keys**. The crash window is "the side effect happened, the result was not recorded." No engine, including Temporal, magically makes a payment exactly-once. They give you a stable identity to dedupe with.
- **Determinism, or an equivalent discipline**, so replay takes the same branch.
- **Versioning**. A workflow that has been asleep for a month must still replay against code that has changed. Temporal has `patched()` and worker versioning for this.
- **Continue-as-new / history truncation**. Event histories grow without bound. Long-lived workflows have to roll up into a fresh run.
- **Visibility**. Search, list, cancel, signal, and query across a large number of executions. This is a lot of the commercial product.

### Why the companies are large

Temporal's own announcements, not a market estimate:

- [Series D, 17 Feb 2026](https://temporal.io/blog/temporal-raises-usd300m-series-d-at-a-usd5b-valuation): $300M at a $5B post-money valuation, led by a16z.
- [Series E, 14 Sep 2026](https://temporal.io/blog/temporal-raises-usd550m-series-e-at-usd12-55b-valuation-ai): $550M at a $12.55B valuation. The company said the team had doubled to about 570 people in the prior year.
- a16z's [investment note](https://a16z.com/announcement/investing-in-temporal/) names OpenAI, Replit, Lovable, Abridge, and Anthropic as users of Temporal for both AI and non-AI workflows.

The money is not in "a checkpoint table." It is in being the execution layer that payments, fulfillment, onboarding, and now multi-step agents sit on, with a hosted control plane, retention, and an on-call story. Adjacent products (Restate, DBOS, Inngest, Trigger.dev, Orkes Conductor, Cloudflare Workflows, AWS Lambda durable functions, Azure Durable Functions) are the same market sliced by operations model: heavy cluster, library-plus-Postgres, or fully hosted steps.

LangGraph already occupies the adjacent "durable agent" niche. The [persistence docs](https://docs.langchain.com/oss/javascript/langgraph/persistence) and the [Deep Agents durability note](https://docs.langchain.com/oss/javascript/deepagents/going-to-production#durability) describe checkpointed steps, interrupts that can last for days, and time travel. That is real. It is the agent-shaped subset of durable execution, not the general one.

## Where the thesis breaks

### Delta channels are not a stack

`DeltaChannel` / `DeltaValue` ([implementation](../../libs/langgraph-core/src/channels/delta.ts)) avoids rewriting a whole accumulated value on every superstep. The checkpoint stores the writes. A read folds them with a reducer, and every so often a full snapshot is stored so the fold stays bounded (`snapshotFrequency`, plus a cap on supersteps since the last snapshot).

That is a storage optimization for reducer state (message lists, logs, counters). It requires the reducer to be deterministic and batching-invariant, because LangGraph may fold a bigger batch on replay than the one that was originally written.

It does not store:

- local variables
- the program counter
- the call stack
- which line inside a node had already run

A node that crashes before it returns does not produce a delta write. On resume the node function starts at the top, with new locals. The POC demonstrates this: the random id from the crashed attempt is absent from the reconstructed channel, and the node body runs twice.

So "store program state in delta channels and resume from that point" swaps two different mechanisms. The channel can remember the **outputs** of finished steps. It cannot remember **where inside a function you were**.

### A node is the atomic unit

LangGraph's own fault-tolerance docs say this directly: checkpoints exist at node boundaries, and a failed node starts over. `interrupt()` is the same shape. The resume value is delivered by **re-entering the node**; code before `interrupt()` runs again. The POC's interrupt test records `before-interrupt` twice and `after-interrupt` once.

Pending writes (see `putWrites` in the Pregel loop) are what make a *sibling* that already finished in the same superstep safe to skip. They are not a statement-level journal.

### `task()` is the closest thing to Temporal, and it is still a different contract

The functional API (`entrypoint` + `task`) is replay. On resume the entrypoint function runs from the first line. Each `task()` call bumps a counter and, if that call already has a successful write, returns the stored result instead of running the function. That is the same idea as a Temporal activity or a DBOS step.

The differences that matter:

1. **Identity is the call index, not a logical id.** Task ids are derived from the checkpoint, the step, the task name, and the call counter ([`_prepareSingleTask`](../../libs/langgraph-core/src/pregel/algo.ts)). Arguments are not part of the id. Deleting a `pay()` that used to be call 0 makes the next `pay()` reuse call 0's receipt. The POC deletes the first of two `pay` steps and the survivor returns `receipt:order-1` without charging `order-2` again. Temporal has the same footgun, and an entire versioning feature to cope with it. A step helper does not remove it.

2. **Glue code runs again.** Anything between `task()` calls — logging, branching, `Date.now()`, a random id — runs on every resume. The exception-restart test records `glue` and `glue-between` twice, and `charge` once.

3. **The result is handed back before it is durable.** `durability: "sync"` awaits checkpoint I/O **between supersteps**, not between `task()` calls inside one entrypoint. `putWrites` is tracked and only drained when the superstep finishes or the run's `finally` runs. In the POC, when the workflow reads a task result, `putWrites` has not even been called yet (`writesStarted === 0`). A `process.exit` on the first line of the next task therefore loses that result: `charge` runs again. Kill one task later and `charge` has landed, but `reserve` — the call that just returned — runs again. A thrown exception is different: the `finally` block does flush, so a cold restart after a caught error does *not* repeat finished tasks. "The node threw" and "the machine vanished" are different failures. Temporal's model is built around the second one.

4. **Nothing comes back and pulls the workflow.** After a kill, a new process has to be told to `invoke(null)` on that `thread_id`. LangGraph Platform / a LangSmith deployment can do that for deployed agents. The library does not poll for abandoned runs. DBOS does, because that poll is the product.

5. **No durable time.** An interrupt can pause until some other process resumes the thread. The pause is durable only if that other process exists. `setTimeout` inside a node is not.

6. **History does not roll up.** Functional tasks keep their results as pending writes on the entrypoint's checkpoint. Graph steps each add a checkpoint. There is no continue-as-new. A workflow with a very large step count becomes a very large thread. Delta channels make the *state payload* cheaper — in the POC, 46,833 bytes against 362,120 for the same 20 steps, still 22 checkpoints. They do not cap the number of task results you must replay.

### "Each stack frame is a subgraph" does not generalize

A compiled subgraph with `checkpointer: true` has its own checkpoint namespace, so a crash in a later inner node can keep an earlier inner node. The POC does that for a two-node `fulfill` frame.

That is a fixed tree you wrote down in advance. It is not "any function call." You cannot take an arbitrary JS stack, put each frame in a subgraph, and get Temporal. Dynamic recursion, higher-order functions, and third-party code do not become nodes by themselves. Inherited subgraphs are coarser: the parent treats the child as one unit for time travel, and inner progress is an implementation detail you should not depend on for a stack discipline.

## Six ways to instrument code, scored

Feasibility means "a careful developer can get crash-resume for a workflow they fully control." Scalability means "the same design still works for many long-lived executions, large step counts, and deploys that change the code."

### 1. Graph API, one node per step

**What you write.** An explicit `StateGraph`. Every side effect is a node. Every value the next node needs is a channel. Invoke with `durability: "sync"`. On failure, start a worker and `invoke(null)` with the same `thread_id`.

**What the POC shows.** An exception in `ship` re-runs `ship` only. `process.exit` inside `ship` leaves `charge` and `reserve` done exactly once, because sync mode awaits the checkpoint before the next superstep, and resume is `invoke(null)`. The same kill under `durability: "exit"` persists nothing to resume from (`Received no input writes for "__start__"`). Starting the thread again runs `charge` and `reserve` a second time. Exit mode is documented this way: it persists when the run exits, so a hard crash mid-run has nothing to resume from.

**Feasibility.** High, for workflows that are naturally a pipeline or a state machine. This is the approach the thesis describes, minus the delta-channel part. You pay for it by lifting every local into state and by giving up `for` / `if` as the source of truth. Loops become cycles. Branches become conditional edges.

**Scalability.** Medium, and only with a real checkpointer (Postgres, Redis), sync or carefully bounded async durability, and an external supervisor. Each superstep writes a checkpoint, so a 10k-step workflow is 10k writes of the full channel set unless those channels are delta channels. Parallel nodes in one superstep are a good fit (pending writes already skip the siblings that finished). It is a poor fit for chatty, highly branching imperative code. Code changes are safe-ish in a way Temporal's are not: you are not replaying a command log, you are loading the last state and running the next node. Changing a node that already ran does not rewrite history. Changing the *shape* of the state or the graph out from under an in-flight thread still breaks that thread.

### 2. Functional API, `task()` as the journal

**What you write.** An `entrypoint` that looks like normal `async` code. Every side effect, random value, clock read, and branch input goes through `task()`. Glue between tasks must be deterministic. Resume with `invoke(null)`.

**What the POC shows.** After a thrown error, a new process (new `FileSaver` loaded from disk, new closures) returns cached `charge` and `reserve` results and only re-runs `ship`. The lines between tasks (`glue`, `glue-between`) run again. A hard kill is weaker than that exception path. With `durability: "sync"`, `process.exit` on the first line of the next task runs `charge` a second time, and `invoke(null)` still resumes the thread — it just does not have the receipt. Kill one task later and `charge` was durable but `reserve` was not. A slow checkpointer confirms the ordering: the workflow already holds the return value while `putWrites` has not started.

**Feasibility.** High for short, reviewed workflows. This is the best DX of the six, and it is the one that actually resembles Temporal. It is a bad default for arbitrary existing code, because the durability is a convention (what you remembered to wrap), not a property of the runtime.

**Scalability.** Low to medium. Replay re-executes the whole entrypoint on every resume, including after every worker cache miss. Temporal spends real engineering on sticky caches specifically so this replay is not on the hot path. LangGraph has no equivalent for functional tasks. Call-index identity makes deploys dangerous for anything already in flight (the `pay` test). There is no patching primitive. Pending writes grow with the number of tasks. Parallel `Promise.all` of tasks works (the runtime already schedules them), which is a genuine strength for fan-out.

### 3. Delta channels as the program log

**What you write.** A `DeltaValue` channel. Nodes append to it. You hope the log *is* the program.

**What the POC shows.** Twenty steps of a 1,500-character payload produced 22 checkpoints either way. The full-value channel occupied 362,120 serialized bytes. The delta channel occupied 46,833. Same number of superstep checkpoints; smaller payloads. A cold `getState` reconstructs the list. The node that crashed before returning still ran its body twice, and its local id is not in the log.

**Feasibility.** High as a **state** representation. Low as an execution model. You would still need approach 1 or 2 to decide which function runs. Using the delta log as a hand-rolled journal (read the log, skip steps whose ids are present) means you have written a worse DBOS inside a node, and the node is still atomic.

**Scalability.** High for the state payload, which is the part the thesis got right. Replay of a delta channel is O(writes since the last snapshot), so `snapshotFrequency` has to stay bounded or reads get expensive. It does not help task-result history, worker failover, or code versioning.

### 4. Subgraph per stack frame

**What you write.** Each function is a compiled subgraph. Give it `checkpointer: true` if you need a crash in a later inner node to preserve an earlier one. The parent is one node per call.

**What the POC shows.** `charge` (outer) runs once. `ship` (inner) runs twice. `reserve` runs once, both when the subgraph has `checkpointer: true` and when it inherits the parent saver, as long as durability is `sync`. The nested loop really does checkpoint inner nodes for crash resume. That is a stack frame, for a frame you declared statically. It is not a free pass at time travel: the parent still treats an inherited subgraph as one superstep when you fork history. The docs describe that limit; this POC did not re-test time travel.

**Feasibility.** Medium for a handful of known functions, poor as a general instrumentation strategy. JS call stacks are dynamic. You would be building a compiler from functions to subgraphs, and then you would still be missing timers, leases, and versioning.

**Scalability.** Poor past a shallow tree. Each frame is a checkpoint namespace. Deep or recursive calls multiply checkpoints and make time travel hard to reason about (the docs already warn that an inherited subgraph is a single parent superstep). A cycle of calls needs a cycle in the graph, not a stack of subgraphs.

### 5. `interrupt()` as a cooperative yield

**What you write.** Call `interrupt()` after every step you want to persist. An external runner immediately resumes with an empty value, except when a human actually has to answer.

**What the POC shows.** The line before `interrupt()` runs again on resume. A `task()` that finished before the interrupt does not. So interrupt-as-yield only works if the node body is empty of side effects, which means you have reinvented approach 2 and then added a round trip through the caller for every step.

**Feasibility.** High for human approval, which is what it is for. Low as a durability primitive. You also need a process that is alive to call `invoke` again. A durable timer would be "write a row, exit, let a scheduler resume me later." Interrupt gives you the pause, not the scheduler.

**Scalability.** Poor if every step yields to an external runner: each step is a full run boundary, a network hop, and a checkpoint. Fine for the occasional human wait, which is the design point. LangGraph's docs are explicit that the node restarts from the beginning.

### 6. A `step()` helper around `task()`

**What you write.**

```ts
function step<T>(name: string, fn: () => Promise<T> | T): Promise<T> {
  return task(name, async () => fn())();
}
```

Business code stays straight-line. This is the instrumentation the thesis is reaching for.

**What the POC shows.** It works for the clean crash-and-restart case: `charge` once, `ship` twice, final value consistent. It also reproduces the call-index bug. Two `pay` steps run, the workflow throws, and a resumed copy of the code that deleted the first `pay` returns `receipt:order-1` without charging `order-2` again. The helper does not await durability before returning, so it inherits the kill hole from approach 2.

**Feasibility.** High as syntax. The helper is not a new runtime. Making it actually durable across `process.exit` would mean changing LangGraph so a `task()` result is not observed until `putWrites` has resolved, and shipping a supervisor that scans for threads whose latest checkpoint is unfinished. Both are library and service work, not a wrapper.

**Scalability.** Same ceiling as approach 2. A production version would also need stable step ids (name plus a logical key, not a call counter), idempotency keys inside the step, and a ban on nondeterminism outside `step()`. That is DBOS, built out of LangGraph primitives, without DBOS's startup recovery.

## What would have to be true for the thesis to hold

The thesis holds for a **narrow** product: agent and workflow graphs whose steps are nodes, whose checkpointer is a real database, whose durability mode is `sync` (or async plus a tolerance for losing the last superstep), and whose operator restarts abandoned threads. People already ship that. It is not a new finding, and it is not Temporal.

The thesis does not hold for "write normal code, keep locals in delta channels, and crash-resume the stack" without at least:

1. Awaiting checkpoint durability **before** a step's result becomes visible to the next line. Today sync mode waits between supersteps only.
2. A supervisor that discovers unfinished threads and resumes them. The open-source library will not do this by itself.
3. Durable timers and an external resume, if a step is allowed to sleep longer than a process.
4. Step identity that survives inserting or deleting a step, or an explicit patch/version API, plus replay tests.
5. Idempotency as a rule for every step, because the in-flight step will run again. Delta channels do not close that window.
6. History limits (continue-as-new, or snapshot-and-truncate) once step counts leave the hundreds.

Do those, and you have started a durable-execution engine that happens to use LangGraph's checkpointer and Pregel loop. The hard part is the part Temporal charges for. Delta channels are a useful piece of the storage design for bulky reducer state, and they are the wrong place to look for the program counter.
