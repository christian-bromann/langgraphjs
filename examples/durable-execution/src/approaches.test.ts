import { afterAll, describe, expect, it } from "vitest";
import { mkdirSync, writeFileSync } from "node:fs";
import path from "node:path";
import { z } from "zod/v4";
import {
  Annotation,
  Command,
  DeltaValue,
  END,
  MemorySaver,
  START,
  StateGraph,
  StateSchema,
  entrypoint,
  interrupt,
  task,
} from "../../../libs/langgraph-core/src/index.ts";
import {
  EffectJournal,
  FileSaver,
  runWorker,
  saverFootprint,
  scratchDir,
} from "./support.ts";

const measured: Record<string, unknown> = {};

afterAll(() => {
  const outDir = path.resolve(process.cwd(), "results");
  mkdirSync(outDir, { recursive: true });
  writeFileSync(
    path.join(outDir, "measurements.json"),
    JSON.stringify(measured, null, 2)
  );
});

function threadConfig(durability: "sync" | "async" | "exit" = "sync") {
  return {
    configurable: { thread_id: "t" },
    durability,
  };
}

/**
 * Approach 1 — Graph API, one node per step.
 * Each function is a superstep. With durability "sync", the checkpointer is
 * awaited before the next node starts. A crash inside a node re-runs that
 * node only.
 */
describe("approach 1: one graph node per step", () => {
  it("cold-restarts after an exception without repeating finished nodes", async () => {
    const dir = scratchDir("graph-exception");
    const journal = new EffectJournal(path.join(dir, "effects.json"));

    const compile = (checkpointer: FileSaver) => {
      const State = Annotation.Root({
        orderId: Annotation<string>(),
        charged: Annotation<boolean>({
          reducer: (_left, right) => right,
          default: () => false,
        }),
        reserved: Annotation<boolean>({
          reducer: (_left, right) => right,
          default: () => false,
        }),
        shipped: Annotation<boolean>({
          reducer: (_left, right) => right,
          default: () => false,
        }),
        notified: Annotation<boolean>({
          reducer: (_left, right) => right,
          default: () => false,
        }),
      });

      return new StateGraph(State)
        .addNode("charge", (state) => {
          journal.record("charge", state.orderId);
          return { charged: true };
        })
        .addNode("reserve", () => {
          journal.record("reserve");
          return { reserved: true };
        })
        .addNode("ship", () => {
          journal.record("ship");
          if (journal.count("ship") === 1) throw new Error("injected crash");
          return { shipped: true };
        })
        .addNode("notify", () => {
          journal.record("notify");
          return { notified: true };
        })
        .addEdge(START, "charge")
        .addEdge("charge", "reserve")
        .addEdge("reserve", "ship")
        .addEdge("ship", "notify")
        .addEdge("notify", END)
        .compile({ checkpointer });
    };

    const config = threadConfig("sync");
    await expect(
      compile(new FileSaver(path.join(dir, "checkpoints.json"))).invoke(
        { orderId: "o-1" },
        config
      )
    ).rejects.toThrow(/injected crash/);

    const resumed = await compile(
      new FileSaver(path.join(dir, "checkpoints.json"))
    ).invoke(null, config);

    expect(resumed).toMatchObject({
      orderId: "o-1",
      charged: true,
      reserved: true,
      shipped: true,
      notified: true,
    });
    expect(journal.names()).toEqual([
      "charge",
      "reserve",
      "ship",
      "ship",
      "notify",
    ]);
  });

  it("survives process.exit on a later node when durability is sync", async () => {
    const dir = scratchDir("graph-kill-sync");
    const killed = await runWorker("graph-sync-kill", dir);
    expect(killed.code).toBe(42);

    const resumed = await runWorker("graph-sync-resume", dir);
    expect(resumed.code).toBe(0);
    expect(resumed.json).toMatchObject({
      ok: true,
      resumedWith: "null",
      effects: ["charge", "reserve", "ship-enter", "ship:graph"],
    });
    measured.graphSyncKill = resumed.json;
  });

  it("loses finished nodes when durability is exit and the process is killed", async () => {
    const dir = scratchDir("graph-kill-exit");
    const killed = await runWorker("graph-exit-kill", dir);
    expect(killed.code).toBe(42);

    const resumed = await runWorker("graph-exit-resume", dir);
    expect(resumed.code).toBe(0);
    expect(resumed.json).toMatchObject({ ok: true });
    measured.graphExitKill = resumed.json;

    const effects = (resumed.json?.effects ?? []) as string[];
    // Exit mode does not persist supersteps as they finish, so the only way
    // forward is a fresh invocation. charge runs again.
    expect(effects.filter((name) => name === "charge").length).toBe(2);
    expect(resumed.json?.resumedWith).toBe("fresh-input");
  });
});

/**
 * Approach 2 — Functional API. `task()` results are journaled as pending
 * writes and skipped on replay. The entrypoint body itself is ordinary code
 * and runs again from the first line.
 */
describe("approach 2: functional tasks as a journal", () => {
  function compile(checkpointer: FileSaver, journal: EffectJournal) {
    const charge = task("charge", async (orderId: string) => {
      journal.record("charge", orderId);
      return `charged:${orderId}`;
    });
    const reserve = task("reserve", async (receipt: string) => {
      journal.record("reserve");
      return `reserved:${receipt}`;
    });
    const ship = task("ship", async (reservation: string) => {
      journal.record("ship");
      if (journal.count("ship") === 1) throw new Error("injected crash");
      return `shipped:${reservation}`;
    });
    const notify = task("notify", async (shipment: string) => {
      journal.record("notify");
      return `notified:${shipment}`;
    });

    return entrypoint(
      { name: "order", checkpointer },
      async (orderId: string) => {
        journal.record("glue");
        const charged = await charge(orderId);
        journal.record("glue-between");
        const reserved = await reserve(charged);
        const shipped = await ship(reserved);
        const notified = await notify(shipped);
        return { charged, reserved, shipped, notified };
      }
    );
  }

  it("replays completed tasks after a thrown error and a cold restart", async () => {
    const dir = scratchDir("func-exception");
    const journal = new EffectJournal(path.join(dir, "effects.json"));
    const file = path.join(dir, "checkpoints.json");
    const config = threadConfig("sync");

    await expect(
      compile(new FileSaver(file), journal).invoke("o-1", config)
    ).rejects.toThrow(/injected crash/);

    const result = await compile(new FileSaver(file), journal).invoke(
      null,
      config
    );

    expect(result).toEqual({
      charged: "charged:o-1",
      reserved: "reserved:charged:o-1",
      shipped: "shipped:reserved:charged:o-1",
      notified: "notified:shipped:reserved:charged:o-1",
    });
    expect(journal.names()).toEqual([
      "glue",
      "charge",
      "glue-between",
      "reserve",
      "ship",
      "glue",
      "glue-between",
      "ship",
      "notify",
    ]);
  });

  it("does not make a task result durable before the next line runs", async () => {
    const saver = new (class extends MemorySaver {
      writesStarted = 0;

      writesFinished = 0;

      override async putWrites(
        ...args: Parameters<MemorySaver["putWrites"]>
      ): Promise<void> {
        this.writesStarted += 1;
        await new Promise((resolve) => {
          setTimeout(resolve, 30);
        });
        await super.putWrites(...args);
        this.writesFinished += 1;
      }
    })();

    const step = task("step", async (n: number) => n * 2);

    let startedAtReturn = -1;
    let finishedAtReturn = -1;

    const workflow = entrypoint(
      { name: "gap", checkpointer: saver },
      async (n: number) => {
        const value = await step(n);
        startedAtReturn = saver.writesStarted;
        finishedAtReturn = saver.writesFinished;
        return value;
      }
    );

    const result = await workflow.invoke(21, threadConfig("sync"));
    expect(result).toBe(42);
    measured.observedBeforeDurable = { startedAtReturn, finishedAtReturn };

    // The workflow observed `42` before the checkpointer finished (and, if
    // the runner has not even committed yet, before the write started).
    expect(finishedAtReturn).toBeLessThan(saver.writesFinished);
    expect(finishedAtReturn).toBe(0);
  });

  it("loses task results when the process is killed on the next line", async () => {
    const dir = scratchDir("func-kill");
    const killed = await runWorker("func-sync-kill", dir);
    expect(killed.code).toBe(42);

    const resumed = await runWorker("func-sync-resume", dir);
    expect(resumed.code).toBe(0);
    measured.funcSyncKill = resumed.json;

    const effects = (resumed.json?.effects ?? []) as string[];
    // process.exit skips the run's finally-flush. The task that just returned
    // (`reserve`) is not durable yet. The task before that (`charge`) has had
    // a full scheduling turn and is on disk. Glue between tasks runs again.
    expect(resumed.json?.resumedWith).toBe("null");
    expect(effects.filter((name) => name === "charge")).toEqual(["charge"]);
    expect(effects.filter((name) => name === "reserve")).toEqual([
      "reserve",
      "reserve",
    ]);
    expect(effects.filter((name) => name === "glue")).toEqual(["glue", "glue"]);
  });

  it("loses the task that just returned when the next task kills the process", async () => {
    const dir = scratchDir("func-kill-immediate");
    const killed = await runWorker("func-sync-kill-after-charge", dir);
    expect(killed.code).toBe(42);

    const resumed = await runWorker("func-sync-resume", dir);
    expect(resumed.code).toBe(0);
    measured.funcSyncKillImmediate = resumed.json;

    const effects = (resumed.json?.effects ?? []) as string[];
    measured.funcImmediateChargeRuns = effects.filter(
      (name) => name === "charge"
    ).length;
    // Killed on the first line of the next task, before `putWrites` for
    // `charge` has run. Resume has to charge again. `invoke(null)` still
    // works: the thread exists, the side effect does not.
    expect(measured.funcImmediateChargeRuns).toBe(2);
    expect(resumed.json?.resumedWith).toBe("null");
  });
});

/**
 * Approach 3 — Delta channels. They store reducer inputs instead of a full
 * snapshot on every step. They do not store a program counter, local
 * variables, or a call stack.
 */
describe("approach 3: delta channels are state compression, not a stack", () => {
  const payload = "x".repeat(1500);

  function fullState() {
    return Annotation.Root({
      items: Annotation<string[]>({
        reducer: (left, right) => left.concat(right),
        default: () => [],
      }),
    });
  }

  function deltaState() {
    return new StateSchema({
      items: new DeltaValue(z.array(z.string()).default(() => []), {
        inputSchema: z.string(),
        reducer: (current: string[], writes: string[]) => [
          ...current,
          ...writes,
        ],
        snapshotFrequency: 100,
      }),
    });
  }

  it("reconstructs accumulated state and still re-runs the crashed node", async () => {
    const dir = scratchDir("delta");
    const journal = new EffectJournal(path.join(dir, "effects.json"));
    const file = path.join(dir, "checkpoints.json");
    const locals: string[] = [];

    const compile = (checkpointer: FileSaver) => {
      const State = deltaState();
      return new StateGraph(State)
        .addNode("seed", () => ({ items: "seed" }))
        .addNode("mutate", () => {
          const local = `local-${locals.length}-${Math.random()}`;
          locals.push(local);
          journal.record("mutate", local);
          if (journal.count("mutate") === 1) {
            throw new Error("crash before the delta write is returned");
          }
          return { items: local };
        })
        .addEdge(START, "seed")
        .addEdge("seed", "mutate")
        .addEdge("mutate", END)
        .compile({ checkpointer });
    };

    const config = threadConfig("sync");
    await expect(
      compile(new FileSaver(file)).invoke({}, config)
    ).rejects.toThrow(/crash before the delta/);

    const cold = compile(new FileSaver(file));
    const result = await cold.invoke(null, config);
    const viewed = await cold.getState(config);

    expect(journal.count("mutate")).toBe(2);
    expect(locals).toHaveLength(2);
    expect(locals[0]).not.toBe(locals[1]);
    // The crashed attempt's local variable is gone. Only the successful
    // write was folded into the channel.
    expect(result.items).toEqual(["seed", locals[1]]);
    expect(viewed.values.items).toEqual(["seed", locals[1]]);
    expect(result.items).not.toContain(locals[0]);
  });

  it("stores less than a full-value channel across many steps", async () => {
    const steps = 20;

    const fullSaver = new MemorySaver();
    let full = new StateGraph(fullState());
    for (let i = 0; i < steps; i += 1) {
      const name = `n${i}`;
      full = full.addNode(name, () => ({ items: [`${payload}-${i}`] }));
      full = i === 0 ? full.addEdge(START, name) : full.addEdge(`n${i - 1}`, name);
    }
    full = full.addEdge(`n${steps - 1}`, END);
    await full.compile({ checkpointer: fullSaver }).invoke({}, threadConfig("sync"));

    const deltaSaver = new MemorySaver();
    let delta = new StateGraph(deltaState());
    for (let i = 0; i < steps; i += 1) {
      const name = `n${i}`;
      delta = delta.addNode(name, () => ({ items: `${payload}-${i}` }));
      delta =
        i === 0 ? delta.addEdge(START, name) : delta.addEdge(`n${i - 1}`, name);
    }
    delta = delta.addEdge(`n${steps - 1}`, END);
    const deltaGraph = delta.compile({ checkpointer: deltaSaver });
    const deltaResult = await deltaGraph.invoke({}, threadConfig("sync"));

    const fullFoot = saverFootprint(fullSaver);
    const deltaFoot = saverFootprint(deltaSaver);
    measured.checkpointBytes = { steps, payloadChars: payload.length, fullFoot, deltaFoot };

    expect(deltaResult.items).toHaveLength(steps);
    expect(deltaFoot.bytes).toBeLessThan(fullFoot.bytes);
  });
});

/**
 * Approach 4 — a subgraph per call frame.
 * Feasible for a fixed, shallow tree of functions. Not a general stack.
 */
describe("approach 4: subgraph per stack frame", () => {
  async function run(ownCheckpointer: boolean) {
    const dir = scratchDir(ownCheckpointer ? "sub-own" : "sub-inherit");
    const journal = new EffectJournal(path.join(dir, "effects.json"));
    const file = path.join(dir, "checkpoints.json");
    const prefix = ownCheckpointer ? "own" : "inherit";

    // Separate last-value channels. A concat reducer would run again on the
    // subgraph's full output and count the outer step twice.
    const State = Annotation.Root({
      charge: Annotation<string>({
        reducer: (_left, right) => right,
        default: () => "",
      }),
      reserve: Annotation<string>({
        reducer: (_left, right) => right,
        default: () => "",
      }),
      ship: Annotation<string>({
        reducer: (_left, right) => right,
        default: () => "",
      }),
    });

    const compile = (checkpointer: FileSaver) => {
      const inner = new StateGraph(State)
        .addNode("reserveNode", () => {
          journal.record(`${prefix}:reserve`);
          return { reserve: "reserved" };
        })
        .addNode("shipNode", () => {
          journal.record(`${prefix}:ship`);
          if (journal.count(`${prefix}:ship`) === 1) {
            throw new Error("crash in inner frame");
          }
          return { ship: "shipped" };
        })
        .addEdge(START, "reserveNode")
        .addEdge("reserveNode", "shipNode")
        .addEdge("shipNode", END)
        .compile(ownCheckpointer ? { checkpointer: true } : {});

      return new StateGraph(State)
        .addNode("chargeNode", () => {
          journal.record(`${prefix}:charge`);
          return { charge: "charged" };
        })
        .addNode("fulfill", inner)
        .addEdge(START, "chargeNode")
        .addEdge("chargeNode", "fulfill")
        .addEdge("fulfill", END)
        .compile({ checkpointer });
    };

    const config = threadConfig("sync");
    await expect(
      compile(new FileSaver(file)).invoke({ trace: [] }, config)
    ).rejects.toThrow(/crash in inner frame/);

    const result = await compile(new FileSaver(file)).invoke(null, config);
    return {
      charge: result.charge,
      reserve: result.reserve,
      ship: result.ship,
      effects: journal.names(),
    };
  }

  it("keeps the outer frame and the finished inner node", async () => {
    const owned = await run(true);
    const inherited = await run(false);
    measured.subgraph = { owned: owned.effects, inherited: inherited.effects };

    expect(owned.effects.filter((name) => name === "own:charge")).toEqual([
      "own:charge",
    ]);
    expect(owned.effects.filter((name) => name === "own:reserve")).toEqual([
      "own:reserve",
    ]);
    expect(owned.effects.filter((name) => name === "own:ship")).toHaveLength(2);
    expect(owned).toMatchObject({
      charge: "charged",
      reserve: "reserved",
      ship: "shipped",
    });

    expect(
      inherited.effects.filter((name) => name === "inherit:charge")
    ).toEqual(["inherit:charge"]);
    expect(inherited.charge).toBe("charged");
    expect(inherited.ship).toBe("shipped");
    measured.inheritedReserveRuns = inherited.effects.filter(
      (name) => name === "inherit:reserve"
    ).length;
    // Under durability "sync", the nested loop checkpoints inner nodes even
    // when the subgraph inherits the parent saver. Time travel from the parent
    // is coarser than this; crash resume is not.
    expect(measured.inheritedReserveRuns).toBe(1);
    expect(
      inherited.effects.filter((name) => name === "inherit:ship")
    ).toHaveLength(2);
  });
});

/**
 * Approach 5 — interrupt() as a yield point.
 * The node starts over on resume. interrupt() is not a program counter.
 */
describe("approach 5: interrupt is not a program counter", () => {
  it("re-runs code that sits before interrupt()", async () => {
    const dir = scratchDir("interrupt");
    const journal = new EffectJournal(path.join(dir, "effects.json"));
    const saver = new FileSaver(path.join(dir, "checkpoints.json"));

    const State = Annotation.Root({
      answer: Annotation<string>({
        reducer: (_left, right) => right,
        default: () => "",
      }),
    });

    const graph = new StateGraph(State)
      .addNode("ask", () => {
        journal.record("before-interrupt");
        const answer = interrupt("approve?");
        journal.record("after-interrupt");
        return { answer: String(answer) };
      })
      .addEdge(START, "ask")
      .addEdge("ask", END)
      .compile({ checkpointer: saver });

    const config = threadConfig("sync");
    const paused = await graph.invoke({}, config);
    expect(paused.__interrupt__?.[0]?.value).toBe("approve?");
    expect(journal.names()).toEqual(["before-interrupt"]);

    const done = await graph.invoke(new Command({ resume: "yes" }), config);
    expect(done.answer).toBe("yes");
    expect(journal.names()).toEqual([
      "before-interrupt",
      "before-interrupt",
      "after-interrupt",
    ]);
  });

  it("does not re-run a task() that finished before the interrupt", async () => {
    const dir = scratchDir("interrupt-task");
    const journal = new EffectJournal(path.join(dir, "effects.json"));
    const saver = new FileSaver(path.join(dir, "checkpoints.json"));

    const work = task("work", async () => {
      journal.record("task-work");
      return "ok";
    });

    const State = Annotation.Root({
      answer: Annotation<string>({
        reducer: (_left, right) => right,
        default: () => "",
      }),
    });

    const graph = new StateGraph(State)
      .addNode("ask", async () => {
        const value = await work();
        const answer = interrupt(`approve ${value}?`);
        return { answer: String(answer) };
      })
      .addEdge(START, "ask")
      .addEdge("ask", END)
      .compile({ checkpointer: saver });

    const config = threadConfig("sync");
    await graph.invoke({}, config);
    const done = await graph.invoke(new Command({ resume: "yes" }), config);

    expect(done.answer).toBe("yes");
    expect(journal.names()).toEqual(["task-work"]);
  });
});

/**
 * Approach 6 — a step() helper, the practical way to instrument imperative
 * code. It is approach 2 with a thinner syntax, including the call-index
 * hazard.
 */
describe("approach 6: step() instrumentation and the call-index hazard", () => {
  it("reads like straight-line code and skips finished steps after a crash", async () => {
    const dir = scratchDir("step-helper");
    const journal = new EffectJournal(path.join(dir, "effects.json"));
    const file = path.join(dir, "checkpoints.json");

    function step<T>(name: string, fn: () => Promise<T> | T): Promise<T> {
      return task(name, async () => fn())();
    }

    const compile = (checkpointer: FileSaver) =>
      entrypoint({ name: "checkout", checkpointer }, async (orderId: string) => {
        const charged = await step("charge", () => {
          journal.record("charge", orderId);
          return `charged:${orderId}`;
        });
        const shipped = await step("ship", () => {
          journal.record("ship");
          if (journal.count("ship") === 1) throw new Error("injected crash");
          return `shipped:${charged}`;
        });
        return { charged, shipped };
      });

    const config = threadConfig("sync");
    await expect(
      compile(new FileSaver(file)).invoke("o-9", config)
    ).rejects.toThrow(/injected crash/);

    const result = await compile(new FileSaver(file)).invoke(null, config);
    expect(result).toEqual({
      charged: "charged:o-9",
      shipped: "shipped:charged:o-9",
    });
    expect(journal.names()).toEqual(["charge", "ship", "ship"]);
  });

  it("replays the wrong receipt if a same-named step is deleted in front", async () => {
    const dir = scratchDir("call-index");
    const journal = new EffectJournal(path.join(dir, "effects.json"));
    const file = path.join(dir, "checkpoints.json");
    const config = threadConfig("sync");

    const pay = (orderId: string) =>
      task("pay", async (id: string) => {
        journal.record(`pay:${id}`);
        return `receipt:${id}`;
      })(orderId);

    await expect(
      entrypoint(
        { name: "pay-workflow", checkpointer: new FileSaver(file) },
        async () => {
          const first = await pay("order-1");
          const second = await pay("order-2");
          throw new Error(`crash after ${first} and ${second}`);
        }
      ).invoke("go", config)
    ).rejects.toThrow(/crash after/);

    expect(journal.names()).toEqual(["pay:order-1", "pay:order-2"]);

    const edited = await entrypoint(
      { name: "pay-workflow", checkpointer: new FileSaver(file) },
      async () => {
        // The first charge was deleted. This call now occupies index 0.
        const second = await pay("order-2");
        return { second };
      }
    ).invoke(null, config);

    measured.callIndex = { edited, effects: journal.names() };

    expect(edited).toEqual({ second: "receipt:order-1" });
    expect(journal.names()).toEqual(["pay:order-1", "pay:order-2"]);
  });
});
