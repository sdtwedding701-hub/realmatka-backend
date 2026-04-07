import { createSession, findUserByPhone, getUserBalance, requireUserByToken, verifyCredential } from "../db.mjs";
import { corsPreflight, fail, getJsonBody, getSessionToken, normalizeIndianPhone, ok, unauthorized } from "../http.mjs";

export function options(request) {
  return corsPreflight(request);
}

export async function login(request) {
  const body = await getJsonBody(request);
  const rawPhone = String(body.phone ?? "");
  const phone = normalizeIndianPhone(rawPhone) ?? rawPhone.trim();
  const password = String(body.password ?? "");

  const user = await findUserByPhone(phone);
  if (!user || !verifyCredential(password, user.passwordHash)) {
    return fail("Invalid phone or password", 401, request);
  }

  if (user.deactivatedAt) {
    return fail("Your account is deactivated. Contact support.", 403, request);
  }
  if (user.blockedAt) {
    return fail("Your account is blocked. Contact support.", 403, request);
  }

  if (user.approvalStatus !== "Approved") {
    return fail(
      user.approvalStatus === "Rejected"
        ? "Your account registration was rejected. Contact support."
        : "Your account is pending admin approval.",
      403,
      request
    );
  }

  const { rawToken } = await createSession(user.id);
  return ok(
    {
      token: rawToken,
      user: {
        id: user.id,
        phone: user.phone,
        name: user.name,
        role: user.role,
        referralCode: user.referralCode,
        joinedAt: user.joinedAt
      }
    },
    request
  );
}

export async function me(request) {
  const user = await requireUserByToken(getSessionToken(request));
  if (!user) {
    return unauthorized(request);
  }

  return ok(
    {
      id: user.id,
      phone: user.phone,
      name: user.name,
      role: user.role,
      referralCode: user.referralCode,
      joinedAt: user.joinedAt,
      walletBalance: await getUserBalance(user.id)
    },
    request
  );
}
