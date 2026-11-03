import type { GeoPoint } from "./order.js";

export type Vehicle = "BIKE" | "MOTO" | "CAR";

export interface Pulser {
  id: string;
  name: string;
  location: GeoPoint;
  rating: number;
  activeTasks: number;
  maxTasks: number;
  vehicle: Vehicle;
  online: boolean;
}

export interface Assignment {
  orderId: string;
  pulserId: string;
  score: number;
  etaMinutes: number;
  assignedAt: string;
}

const AVERAGE_SPEED_KMH: Record<Vehicle, number> = { BIKE: 14, MOTO: 28, CAR: 22 };

/** Great-circle distance in kilometres. */
export function haversineKm(a: GeoPoint, b: GeoPoint): number {
  const toRad = (deg: number) => (deg * Math.PI) / 180;
  const dLat = toRad(b.lat - a.lat);
  const dLng = toRad(b.lng - a.lng);
  const h =
    Math.sin(dLat / 2) ** 2 + Math.cos(toRad(a.lat)) * Math.cos(toRad(b.lat)) * Math.sin(dLng / 2) ** 2;
  return 2 * 6371 * Math.asin(Math.sqrt(h));
}

export function etaMinutes(pulser: Pulser, target: GeoPoint): number {
  const km = haversineKm(pulser.location, target);
  return Math.max(1, Math.round((km / AVERAGE_SPEED_KMH[pulser.vehicle]) * 60));
}

export function isAvailable(pulser: Pulser): boolean {
  return pulser.online && pulser.activeTasks < pulser.maxTasks;
}

/**
 * Higher is better. Favors short ETAs, good ratings and spare capacity so load spreads
 * across the fleet instead of piling onto the single closest pulser.
 */
export function scorePulser(pulser: Pulser, target: GeoPoint): number {
  const eta = etaMinutes(pulser, target);
  const etaScore = 1 / (1 + eta / 10);
  const ratingScore = pulser.rating / 5;
  const capacityScore = 1 - pulser.activeTasks / pulser.maxTasks;
  return Number((0.5 * etaScore + 0.3 * ratingScore + 0.2 * capacityScore).toFixed(4));
}
