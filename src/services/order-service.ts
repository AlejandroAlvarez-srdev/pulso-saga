import type { AnyMessage } from "../domain/messages.js";
import { validateOrder, type Order } from "../domain/order.js";
import { Streams } from "../messaging/bus.js";
import type { Random } from "../shared/random.js";
import { reply, UnsupportedCommandError, type Participant } from "./participant.js";

export interface OrderServiceOptions {
  /** Probability that the merchant withdraws the order before confirmation (business failure). */
  merchantWithdrawRate?: number;
  random?: Random;
}

export class OrderService implements Participant {
  readonly name = "order-service";
  readonly stream = Streams.orderCommands;
  private readonly orders = new Map<string, Order>();
  /** Orders cancelled before they were created; a late create must not resurrect them. */
  private readonly tombstones = new Set<string>();
  private readonly random: Random;

  constructor(private readonly options: OrderServiceOptions = {}) {
    this.random = options.random ?? Math.random;
  }

  get(orderId: string): Order | undefined {
    return this.orders.get(orderId);
  }

  handle(command: AnyMessage): Promise<AnyMessage[]> {
    switch (command.type) {
      case "order.create": {
        const { order } = command.payload;
        if (this.tombstones.has(order.id)) {
          return Promise.resolve([reply(command, "order.rejected", { orderId: order.id, reason: "order_cancelled" })]);
        }
        const errors = validateOrder(order);
        if (errors.length > 0) {
          return Promise.resolve([reply(command, "order.rejected", { orderId: order.id, reason: errors.join("; ") })]);
        }
        const stored = this.orders.get(order.id) ?? { ...order, status: "PAYMENT_PENDING" as const };
        this.orders.set(order.id, stored);
        return Promise.resolve([reply(command, "order.created", { orderId: order.id, status: stored.status })]);
      }
      case "order.confirm": {
        const { orderId, pulserId } = command.payload;
        const order = this.orders.get(orderId);
        if (!order || order.status === "CANCELLED") {
          return Promise.resolve([reply(command, "order.confirm_failed", { orderId, reason: "order_not_active" })]);
        }
        if (order.status !== "ASSIGNED" && this.random() < (this.options.merchantWithdrawRate ?? 0)) {
          return Promise.resolve([reply(command, "order.confirm_failed", { orderId, reason: "merchant_withdrew" })]);
        }
        order.status = "ASSIGNED";
        return Promise.resolve([reply(command, "order.confirmed", { orderId, pulserId, status: order.status })]);
      }
      case "order.cancel": {
        const { orderId, reason } = command.payload;
        const order = this.orders.get(orderId);
        if (order) order.status = "CANCELLED";
        else this.tombstones.add(orderId);
        return Promise.resolve([reply(command, "order.cancelled", { orderId, reason })]);
      }
      default:
        return Promise.reject(new UnsupportedCommandError(this.name, command.type));
    }
  }
}
