import { getBidsForUser, requireUserByToken } from "../db.mjs";
import { corsPreflight, getSessionToken, ok, unauthorized } from "../http.mjs";

export function options(request) {
  return corsPreflight(request);
}

export async function history(request) {
  const user = await requireUserByToken(getSessionToken(request));
  if (!user) {
    return unauthorized(request);
  }

  return ok(await getBidsForUser(user.id), request);
}
