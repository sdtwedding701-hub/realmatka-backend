import { corsPreflight, fail, getJsonBody, getSessionToken, ok, unauthorized } from "../http.mjs";
import { listNotificationsForUser, registerNotificationDevice, requireUserByToken } from "../db.mjs";

export function options(request) {
  return corsPreflight(request);
}

export async function history(request) {
  const user = await requireUserByToken(getSessionToken(request));
  if (!user) {
    return unauthorized(request);
  }

  return ok(await listNotificationsForUser(user.id), request);
}

export async function registerDevice(request) {
  const user = await requireUserByToken(getSessionToken(request));
  if (!user) {
    return unauthorized(request);
  }

  const body = await getJsonBody(request);
  const token = String(body.token ?? "").trim();
  const platform = String(body.platform ?? "android").trim();

  if (!token) {
    return fail("Notification token is required", 400, request);
  }

  const device = await registerNotificationDevice(user.id, platform, token);
  return ok(device, request);
}
