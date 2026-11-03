import { randomUUID } from "node:crypto";
import type { Order } from "../domain/order.js";
import type { Pulser, Vehicle } from "../domain/pulser.js";
import type { Random } from "../shared/random.js";

/** Roughly central Mexico City. */
const CDMX = { lat: 19.4326, lng: -99.1332 };
const NAMES = ["Ana", "Luis", "Sofía", "Diego", "Valeria", "Mateo", "Camila", "Emiliano", "Regina", "Santiago"];
const VEHICLES: Vehicle[] = ["BIKE", "MOTO", "CAR"];
const PRODUCTS = [
  { sku: "SKU-CAFE-500", name: "Café de Chiapas 500g" },
  { sku: "SKU-TORT-1KG", name: "Tortillas de maíz 1kg" },
  { sku: "SKU-AGUA-6PK", name: "Agua mineral 6-pack" },
  { sku: "SKU-PAN-DULCE", name: "Pan dulce surtido" },
];

const jitter = (random: Random, spread: number) => (random() - 0.5) * spread;

export function makePulsers(count: number, random: Random): Pulser[] {
  return Array.from({ length: count }, (_, i) => ({
    id: `pulser-${String(i + 1).padStart(3, "0")}`,
    name: `${NAMES[i % NAMES.length] ?? "Pulser"} ${String(i + 1)}`,
    location: { lat: CDMX.lat + jitter(random, 0.15), lng: CDMX.lng + jitter(random, 0.15) },
    rating: Number((3.5 + random() * 1.5).toFixed(2)),
    activeTasks: 0,
    maxTasks: 2 + Math.floor(random() * 3),
    vehicle: VEHICLES[Math.floor(random() * VEHICLES.length)] ?? "MOTO",
    online: random() > 0.1,
  }));
}

export function makeOrder(random: Random, overrides: Partial<Order> = {}): Order {
  const product = PRODUCTS[Math.floor(random() * PRODUCTS.length)] ?? { sku: "SKU-GENERIC", name: "Artículo" };
  return {
    id: randomUUID(),
    merchantId: `merchant-${String(1 + Math.floor(random() * 20))}`,
    status: "CREATED",
    pickup: { lat: CDMX.lat + jitter(random, 0.1), lng: CDMX.lng + jitter(random, 0.1), address: "Av. Insurgentes Sur 1602, CDMX" },
    dropoff: { lat: CDMX.lat + jitter(random, 0.1), lng: CDMX.lng + jitter(random, 0.1), address: "Calle Durango 200, Roma Nte., CDMX" },
    items: [{ ...product, qty: 1 + Math.floor(random() * 3) }],
    amountCents: 5_000 + Math.floor(random() * 45_000),
    currency: "MXN",
    createdAt: new Date().toISOString(),
    ...overrides,
  };
}
