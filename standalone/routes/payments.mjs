import { corsPreflight, fail, getJsonBody, getSessionToken, ok, unauthorized } from "../http.mjs";
import { createPaymentOrder, handlePaymentWebhook, requireUserByToken } from "../db.mjs";

export function options(request) {
  return corsPreflight(request);
}

export async function createOrder(request) {
  const user = await requireUserByToken(getSessionToken(request));
  if (!user) {
    return unauthorized(request);
  }

  const body = await getJsonBody(request);
  const amount = Number(body.amount ?? 0);
  if (amount <= 0) {
    return fail("Amount must be greater than 0", 400, request);
  }

  return ok(await createPaymentOrder({ userId: user.id, amount }), request);
}

export async function webhook(request) {
  const body = await getJsonBody(request);
  const reference = String(body.reference ?? "").trim();
  const status = String(body.status ?? "PENDING").trim();

  if (!reference) {
    return fail("reference is required", 400, request);
  }

  const order = await handlePaymentWebhook(reference, status);
  if (!order) {
    return fail("Payment order not found", 404, request);
  }

  return ok(order, request);
}
