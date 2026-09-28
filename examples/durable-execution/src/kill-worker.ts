/**
 * Child process for the kill tests.
 *
 * `process.exit` skips LangGraph's `finally` block, which is the block that
 * waits for checkpoint writes. That is the difference between "the node threw"
 * and "the machine disappeared."
 *
 * Usage: tsx kill-worker.ts <mode> <dir>
 * Modes: graph-sync-kill, graph-sync-resume, graph-exit-kill, graph-exit-resume,
 *        func-sync-kill, func-sync-kill-after-charge, func-sync-resume
 */
import {
  Annotation,
  END,
  START,
  StateGraph,
  entrypoint,
  task,
} from "../../../libs/langgraph-core/src/index.ts";
import path from "node:path";
import { EffectJournal, FileSaver } from "./support.ts";

const mode = process.argv[2];
const dir = process.argv[3];

if (mode === undefined || dir === undefined) {
  throw new Error("usage: kill-worker.ts <mode> <dir>");
}

const journal = new EffectJournal(path.join(dir, "effects.json"));
const saver = new FileSaver(path.join(dir, "checkpoints.json"));
const thread = {
  configurable: { thread_id: "kill" },
};

function emit(payload: Record<string, unknown>): void {
  process.stdout.write(
    `${JSON.stringify({ mode, effects: journal.names(), ...payload })}\n`
  );
}

function dieOnce(marker: string): void {
  const killing = mode.includes("-kill");
  if (killing && journal.count(marker) === 0) {
    journal.record(marker);
    process.exit(42);
  }
}

function compileGraph() {
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
      dieOnce("ship-enter");
      journal.record("ship:graph");
      return { shipped: true };
    })
    .addEdge(START, "charge")
    .addEdge("charge", "reserve")
    .addEdge("reserve", "ship")
    .addEdge("ship", END)
    .compile({ checkpointer: saver });
}

function compileFunctional() {
  const charge = task("charge", async (orderId: string) => {
    journal.record("charge", orderId);
    return `charged:${orderId}`;
  });
  const reserve = task("reserve", async (receipt: string) => {
    if (mode === "func-sync-kill-after-charge") dieOnce("reserve-enter");
    journal.record("reserve");
    return `reserved:${receipt}`;
  });
  const ship = task("ship", async (reservation: string) => {
    if (mode !== "func-sync-kill-after-charge") dieOnce("ship-enter");
    journal.record("ship:func");
    return `shipped:${reservation}`;
  });

  return entrypoint(
    { name: "order", checkpointer: saver },
    async (orderId: string) => {
      journal.record("glue");
      const charged = await charge(orderId);
      journal.record("glue-between");
      const reserved = await reserve(charged);
      const shipped = await ship(reserved);
      return { charged, reserved, shipped };
    }
  );
}

const durability = mode.includes("exit") ? "exit" : "sync";
const config = { ...thread, durability } as const;

try {
  if (mode.startsWith("graph-") && mode.endsWith("-kill")) {
    await compileGraph().invoke({ orderId: "o-1" }, config);
    emit({ ok: true, phase: "kill-returned" });
  } else if (mode.startsWith("graph-") && mode.endsWith("-resume")) {
    const graph = compileGraph();
    try {
      const result = await graph.invoke(null, config);
      const state = await graph.getState(thread);
      emit({
        ok: true,
        resumedWith: "null",
        result,
        next: state.next,
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      const result = await graph.invoke({ orderId: "o-1" }, config);
      emit({
        ok: true,
        resumedWith: "fresh-input",
        nullError: message,
        result,
      });
    }
  } else if (mode === "func-sync-kill" || mode === "func-sync-kill-after-charge") {
    await compileFunctional().invoke("o-1", config);
    emit({ ok: true, phase: "kill-returned" });
  } else if (mode === "func-sync-resume") {
    const workflow = compileFunctional();
    try {
      const result = await workflow.invoke(null, config);
      emit({ ok: true, resumedWith: "null", result });
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      const result = await workflow.invoke("o-1", config);
      emit({
        ok: true,
        resumedWith: "fresh-input",
        nullError: message,
        result,
      });
    }
  } else {
    throw new Error(`unknown mode ${mode}`);
  }
} catch (error) {
  emit({
    ok: false,
    error: error instanceof Error ? error.stack ?? error.message : String(error),
  });
  process.exitCode = 1;
}
