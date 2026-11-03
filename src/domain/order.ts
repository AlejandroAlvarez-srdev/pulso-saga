export type OrderStatus =
  | "CREATED"
  | "PAYMENT_PENDING"
  | "PAID"
  | "ASSIGNED"
  | "IN_PROGRESS"
  | "DELIVERED"
  | "CANCELLED"
  | "FAILED";

export interface GeoPoint {
  lat: number;
  lng: number;
}

export interface Address extends GeoPoint {
  address: string;
}

export interface OrderItem {
  sku: string;
  name: string;
  qty: number;
}

export interface Order {
  id: string;
  merchantId: string;
  status: OrderStatus;
  pickup: Address;
  dropoff: Address;
  items: OrderItem[];
  amountCents: number;
  currency: "MXN";
  createdAt: string;
}

/** Validation errors are business failures: they end the saga, they are never retried. */
export function validateOrder(order: Order): string[] {
  const errors: string[] = [];
  if (order.items.length === 0) errors.push("order has no items");
  if (order.items.some((item) => item.qty <= 0)) errors.push("item quantity must be positive");
  if (!Number.isInteger(order.amountCents) || order.amountCents <= 0) {
    errors.push("amountCents must be a positive integer");
  }
  // Orders arrive over the wire, so the literal type is not a runtime guarantee.
  if ((order.currency as string) !== "MXN") errors.push("only MXN is supported");
  return errors;
}
