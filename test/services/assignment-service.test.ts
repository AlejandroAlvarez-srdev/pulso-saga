import { describe, expect, it } from "vitest";
import type { AnyMessage } from "../../src/domain/messages.js";
import type { Pulser } from "../../src/domain/pulser.js";
import { createMessage } from "../../src/messaging/message.js";
import { Bulkhead } from "../../src/resilience/bulkhead.js";
import { BulkheadRejectedError } from "../../src/resilience/errors.js";
import { AssignmentService, FakeDispatchEngine, type DispatchEngine } from "../../src/services/assignment-service.js";
import { makeOrder } from "../../src/services/fixtures.js";
import { seededRandom } from "../../src/shared/random.js";

const random = seededRandom(11);
const pickup = { lat: 19.4326, lng: -99.1332 };

const pulser = (id: string, overrides: Partial<Pulser> = {}): Pulser => ({
  id,
  name: id,
  location: pickup,
  rating: 4.5,
  activeTasks: 0,
  maxTasks: 1,
  vehicle: "MOTO",
  online: true,
  ...overrides,
});

const assign = (orderId?: string): AnyMessage => {
  const order = makeOrder(random, { pickup: { ...pickup, address: "Zócalo" }, ...(orderId ? { id: orderId } : {}) });
  return createMessage("pulser.assign", order.id, { order });
};

function service(pulsers: Pulser[], engine: DispatchEngine = new FakeDispatchEngine({ min: 0, max: 0 }), maxConcurrent = 2) {
  return new AssignmentService({
    pulsers,
    engine,
    bulkhead: new Bulkhead({ name: "dispatch", maxConcurrent, maxQueue: 0 }),
    callTimeoutMs: 100,
  });
}

describe("AssignmentService", () => {
  it("assigns the best-scored available pulser and reserves capacity", async () => {
    const s = service([
      pulser("far", { location: { lat: 19.6, lng: -99.3 } }),
      pulser("near-busy", { activeTasks: 1 }),
      pulser("near"),
      pulser("offline", { online: false }),
    ]);
    const [reply] = await s.handle(assign());
    expect(reply?.type).toBe("pulser.assigned");
    expect(reply?.type === "pulser.assigned" && reply.payload.pulserId).toBe("near");
    expect(s.pulser("near")?.activeTasks).toBe(1);
  });

  it("replies assignment.failed when nobody is available", async () => {
    const s = service([pulser("busy", { activeTasks: 1 })]);
    const [reply] = await s.handle(assign());
    expect(reply?.type).toBe("assignment.failed");
  });

  it("release frees capacity and blocks a later assign for the same order", async () => {
    const s = service([pulser("p1")]);
    const command = assign("order-x");
    await s.handle(command);
    const [released] = await s.handle(createMessage("pulser.release", "order-x", { orderId: "order-x" }));
    expect(released?.payload).toEqual({ orderId: "order-x", pulserId: "p1" });
    expect(s.pulser("p1")?.activeTasks).toBe(0);
    const [again] = await s.handle(assign("order-x"));
    expect(again?.type).toBe("assignment.failed");
  });

  it("rejects calls beyond the bulkhead instead of piling onto a slow engine", async () => {
    const gates: (() => void)[] = [];
    const slow: DispatchEngine = {
      rank: () =>
        new Promise((resolve) => {
          gates.push(() => {
            resolve([]);
          });
        }),
    };
    const s = service([pulser("p1")], slow, 1);
    const first = s.handle(assign());
    await expect(s.handle(assign())).rejects.toBeInstanceOf(BulkheadRejectedError);
    gates.forEach((open) => {
      open();
    });
    await first;
  });
});
