import { getUserBalance, requireUserByToken, updateUserProfile } from "../db.mjs";
import { corsPreflight, fail, getJsonBody, getSessionToken, normalizeIndianPhone, ok, unauthorized } from "../http.mjs";

export function options(request) {
  return corsPreflight(request);
}

export async function update(request) {
  const user = await requireUserByToken(getSessionToken(request));
  if (!user) {
    return unauthorized(request);
  }

  const body = await getJsonBody(request);
  const name = String(body.name ?? "").trim();
  const phone = normalizeIndianPhone(String(body.phone ?? "")) ?? String(body.phone ?? "").trim();

  if (!name || !phone) {
    return fail("name and phone are required", 400, request);
  }

  const updated = await updateUserProfile(user.id, { name, phone });
  if (!updated) {
    return fail("User not found", 404, request);
  }

  return ok(
    {
      id: updated.id,
      phone: updated.phone,
      name: updated.name,
      referralCode: updated.referralCode,
      joinedAt: updated.joinedAt,
      walletBalance: await getUserBalance(updated.id)
    },
    request
  );
}
