import { createSession, findUserByPhone, hashCredential, updateUserPassword } from "../db.mjs";
import { corsPreflight, fail, getJsonBody, normalizeIndianPhone, ok } from "../http.mjs";

const challenges = new Map();

function getRequestFingerprint(request, namespace, value = "") {
  const forwarded = request.headers.get("x-forwarded-for")?.split(",")[0]?.trim() ?? "";
  const realIp = request.headers.get("x-real-ip")?.trim() ?? "";
  const userAgent = request.headers.get("user-agent")?.trim() ?? "";
  return [namespace, value, forwarded || realIp || "local", userAgent.slice(0, 80)].join(":");
}

const rateLimitBuckets = new Map();
function assertRateLimit({ key, windowMs, max }) {
  const now = Date.now();
  const entry = rateLimitBuckets.get(key);
  if (!entry || entry.resetAt <= now) {
    rateLimitBuckets.set(key, { count: 1, resetAt: now + windowMs });
    return { allowed: true, retryAfterSeconds: 0 };
  }
  if (entry.count >= max) {
    return { allowed: false, retryAfterSeconds: Math.max(1, Math.ceil((entry.resetAt - now) / 1000)) };
  }
  entry.count += 1;
  rateLimitBuckets.set(key, entry);
  return { allowed: true, retryAfterSeconds: 0 };
}

function createOtpCode() {
  return String(Math.floor(100000 + Math.random() * 900000));
}

async function sendOtp(phone, purpose) {
  const code = createOtpCode();
  const expiresAt = new Date(Date.now() + 10 * 60 * 1000).toISOString();
  challenges.set(`${phone}:${purpose}`, { code, expiresAt });
  return {
    sent: true,
    expiresAt,
    provider: "local",
    devCode: code
  };
}

export async function verifyOtp(phone, purpose, code) {
  const challenge = challenges.get(`${phone}:${purpose}`);
  if (!challenge) {
    return false;
  }
  if (new Date(challenge.expiresAt).getTime() < Date.now()) {
    challenges.delete(`${phone}:${purpose}`);
    return false;
  }
  if (challenge.code !== code) {
    return false;
  }
  challenges.delete(`${phone}:${purpose}`);
  return true;
}

export function options(request) {
  return corsPreflight(request);
}

export async function requestOtp(request) {
  const body = await getJsonBody(request);
  const phone = normalizeIndianPhone(String(body.phone ?? "")) ?? String(body.phone ?? "").trim();
  const rawPurpose = String(body.purpose ?? "login");
  const purpose = rawPurpose === "password_reset" ? "password_reset" : rawPurpose === "register" ? "register" : "login";
  const rateLimit = assertRateLimit({
    key: getRequestFingerprint(request, "auth-request-otp", `${phone}:${purpose}`),
    windowMs: 10 * 60 * 1000,
    max: 5
  });

  if (!rateLimit.allowed) {
    return fail(`Too many OTP requests. Try again in ${rateLimit.retryAfterSeconds}s.`, 429, request);
  }

  if (!phone) {
    return fail("Phone number must be a valid 10 digit Indian mobile number", 400, request);
  }

  const user = await findUserByPhone(phone);
  if (!user && purpose !== "register") {
    return fail("User not found", 404, request);
  }

  if (purpose === "register" && user) {
    return fail("Phone number already registered", 400, request);
  }

  if (purpose === "login" && user?.deactivatedAt) {
    return fail("Your account is deactivated. Contact support.", 403, request);
  }
  if (purpose === "login" && user?.blockedAt) {
    return fail("Your account is blocked. Contact support.", 403, request);
  }

  if (purpose === "login" && user && user.approvalStatus !== "Approved") {
    return fail(
      user.approvalStatus === "Rejected"
        ? "Your account registration was rejected. Contact support."
        : "Your account is pending admin approval.",
      403,
      request
    );
  }

  try {
    const otpState = await sendOtp(phone, purpose);
    return ok(
      {
        sent: otpState.sent,
        purpose,
        expiresAt: otpState.expiresAt,
        provider: otpState.provider,
        devCode: otpState.devCode
      },
      request
    );
  } catch (error) {
    return fail(error instanceof Error ? error.message : "Unable to send OTP", 500, request);
  }
}

export async function otpLogin(request) {
  const body = await getJsonBody(request);
  const phone = normalizeIndianPhone(String(body.phone ?? "")) ?? String(body.phone ?? "").trim();
  const otp = String(body.otp ?? "").trim();
  const rateLimit = assertRateLimit({
    key: getRequestFingerprint(request, "auth-otp-login", phone),
    windowMs: 10 * 60 * 1000,
    max: 10
  });

  if (!rateLimit.allowed) {
    return fail(`Too many OTP login attempts. Try again in ${rateLimit.retryAfterSeconds}s.`, 429, request);
  }

  if (!phone || !/^[0-9]{6}$/.test(otp)) {
    return fail("Valid phone number and 6 digit OTP are required", 400, request);
  }

  let valid = false;
  try {
    valid = await verifyOtp(phone, "login", otp);
  } catch (error) {
    return fail(error instanceof Error ? error.message : "Unable to verify OTP", 500, request);
  }

  if (!valid) {
    return fail("Invalid or expired OTP", 400, request);
  }

  const user = await findUserByPhone(phone);
  if (!user) {
    return fail("User not found", 404, request);
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

export async function forgotPassword(request) {
  const body = await getJsonBody(request);
  const phone = normalizeIndianPhone(String(body.phone ?? "")) ?? String(body.phone ?? "").trim();
  const otp = String(body.otp ?? "").trim();
  const password = String(body.password ?? "");
  const confirmPassword = String(body.confirmPassword ?? "");
  const rateLimit = assertRateLimit({
    key: getRequestFingerprint(request, "auth-forgot-password", phone),
    windowMs: 10 * 60 * 1000,
    max: 10
  });

  if (!rateLimit.allowed) {
    return fail(`Too many reset attempts. Try again in ${rateLimit.retryAfterSeconds}s.`, 429, request);
  }

  if (!phone || !/^[0-9]{6}$/.test(otp)) {
    return fail("Valid phone number and 6 digit OTP are required", 400, request);
  }

  if (password.length < 8) {
    return fail("Password must be at least 8 characters", 400, request);
  }

  if (password !== confirmPassword) {
    return fail("Password and confirm password must match", 400, request);
  }

  let valid = false;
  try {
    valid = await verifyOtp(phone, "password_reset", otp);
  } catch (error) {
    return fail(error instanceof Error ? error.message : "Unable to verify OTP", 500, request);
  }

  if (!valid) {
    return fail("Invalid or expired OTP", 400, request);
  }

  const user = await findUserByPhone(phone);
  if (!user) {
    return fail("User not found", 404, request);
  }

  await updateUserPassword(user.id, hashCredential(password));
  return ok({ success: true }, request);
}
