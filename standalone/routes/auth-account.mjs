import {
  findUserByPhone,
  hashCredential,
  requireUserByToken,
  revokeSession,
  updateUserMpin,
  updateUserPassword,
  verifyCredential
} from "../db.mjs";
import { corsPreflight, fail, getJsonBody, getSessionToken, ok, unauthorized } from "../http.mjs";

export function options(request) {
  return corsPreflight(request);
}

export async function logout(request) {
  await revokeSession(getSessionToken(request));
  return ok({ success: true }, request);
}

export async function updatePassword(request) {
  const user = await requireUserByToken(getSessionToken(request));
  if (!user) {
    return unauthorized(request);
  }

  const body = await getJsonBody(request);
  const currentPassword = String(body.currentPassword ?? "");
  const password = String(body.password ?? "");
  const confirmPassword = String(body.confirmPassword ?? "");

  if (!currentPassword || !password || !confirmPassword) {
    return fail("currentPassword, password, and confirmPassword are required", 400, request);
  }

  const fullUser = await findUserByPhone(user.phone);
  if (!fullUser || !verifyCredential(currentPassword, fullUser.passwordHash)) {
    return fail("Current password is incorrect", 400, request);
  }

  if (password.length < 8) {
    return fail("Password must be at least 8 characters", 400, request);
  }

  if (password !== confirmPassword) {
    return fail("Password and confirm password must match", 400, request);
  }

  await updateUserPassword(user.id, hashCredential(password));
  return ok({ success: true }, request);
}

export async function updateMpin(request) {
  const user = await requireUserByToken(getSessionToken(request));
  if (!user) {
    return unauthorized(request);
  }

  const body = await getJsonBody(request);
  const pin = String(body.pin ?? "");
  const confirmPin = String(body.confirmPin ?? "");

  if (!pin || !confirmPin) {
    return fail("pin and confirmPin are required", 400, request);
  }

  if (!/^[0-9]{4}$/.test(pin)) {
    return fail("PIN must be exactly 4 digits", 400, request);
  }

  if (pin !== confirmPin) {
    return fail("PIN and confirm PIN must match", 400, request);
  }

  await updateUserMpin(user.id, hashCredential(pin));
  return ok({ success: true }, request);
}
