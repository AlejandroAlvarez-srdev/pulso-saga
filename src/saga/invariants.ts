import type { AssignmentService } from "../services/assignment-service.js";
import type { OrderService } from "../services/order-service.js";
import type { PaymentService } from "../services/payment-service.js";
import { orderSaga } from "./definition.js";
import type { SagaState } from "./state.js";

export interface ParticipantViews {
  orders: Pick<OrderService, "get">;
  payments: Pick<PaymentService, "status">;
  assignments: Pick<AssignmentService, "assignmentFor">;
}

const STEP_ORDER: readonly string[] = orderSaga.map((s) => s.name);

/**
 * Consistency invariants that must hold once the system is quiet (no messages in flight).
 * Shared by the fault-injection simulator and the property-based tests so both check the same
 * contract. Returns human-readable violations; an empty list means consistent.
 */
export function checkConsistency(sagas: readonly SagaState[], views: ParticipantViews): string[] {
  const violations: string[] = [];
  const { orders, payments, assignments } = views;
  let completed = 0;

  for (const s of sagas) {
    const id = s.sagaId;
    const payment = payments.status(id);
    const assignment = assignments.assignmentFor(id);

    // Structural: forward steps run in definition order, and a step is only compensated if it
    // completed, timed out with an unknown outcome, or reported a late success.
    if (s.completedSteps.some((step, i) => STEP_ORDER.indexOf(step) !== i)) {
      violations.push(`${id}: completed steps out of order (${s.completedSteps.join(",")})`);
    }
    for (const step of s.compensatedSteps) {
      const justified =
        s.completedSteps.includes(step) ||
        (s.failure?.step === step && s.failure.reason === "timeout") ||
        s.log.some((e) => e.event === "LATE_SUCCESS" && e.step === step);
      if (!justified) violations.push(`${id}: compensated ${step} which never ran`);
    }

    switch (s.status) {
      case "COMPLETED":
        completed++;
        if (s.completedSteps.length !== STEP_ORDER.length) violations.push(`${id}: completed with ${String(s.completedSteps.length)} steps`);
        if (s.compensatedSteps.length > 0) violations.push(`${id}: completed but has compensations`);
        if (orders.get(id)?.status !== "ASSIGNED") violations.push(`${id}: completed but order not ASSIGNED`);
        if (payment !== "CAPTURED") violations.push(`${id}: completed but payment not CAPTURED (${String(payment)})`);
        if (!assignment) violations.push(`${id}: completed without assignment`);
        break;
      case "COMPENSATED":
        if (!s.failure) violations.push(`${id}: compensated without a recorded failure`);
        if (orders.get(id)?.status === "ASSIGNED") violations.push(`${id}: compensated but order ASSIGNED`);
        if (payment === "AUTHORIZED" || payment === "CAPTURED") violations.push(`${id}: compensated but money held (${payment})`);
        if (assignment) violations.push(`${id}: compensated but pulser still assigned`);
        break;
      case "FAILED":
        // Explicitly needs a human; the log must say why.
        if (!s.log.some((e) => e.event === "SAGA_FAILED")) violations.push(`${id}: FAILED without SAGA_FAILED log entry`);
        break;
      case "RUNNING":
      case "COMPENSATING":
        violations.push(`${id}: stuck in ${s.status}`);
        break;
    }
  }

  const busy = sagas.reduce((n, s) => n + (assignments.assignmentFor(s.sagaId) ? 1 : 0), 0);
  const failedWithPulser = sagas.filter((s) => s.status === "FAILED" && assignments.assignmentFor(s.sagaId)).length;
  if (busy - failedWithPulser !== completed) {
    violations.push(`pulser tasks held ${String(busy)} (minus ${String(failedWithPulser)} failed) != completed ${String(completed)}`);
  }
  return violations;
}
