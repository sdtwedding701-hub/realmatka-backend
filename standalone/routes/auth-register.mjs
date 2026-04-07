import { corsPreflight, fail, getJsonBody, normalizeIndianPhone, ok } from "../http.mjs";
import { hashCredential } from "../db.mjs";
import { createUserAccount } from "../db.mjs";
import { verifyOtp } from "./auth-otp.mjs";

const rateLimitBuckets = new Map();
function getRequestFingerprint(request, namespace, value = "") {
  const forwarded = request.headers.get("x-forwarded-for")?.split(",")[0]?.trim() ?? "";
  const realIp = request.headers.get("x-real-ip")?.trim() ?? "";
  const userAgent = request.headers.get("user-agent")?.trim() ?? "";
  return [namespace, value, forwarded || realIp || "local", userAgent.slice(0, 80)].join(":");
}

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

export function options(request) {
  return corsPreflight(request);
}

export async function register(request) {
  const body = await getJsonBody(request);
  const phone = normalizeIndianPhone(String(body.phone ?? "")) ?? String(body.phone ?? "").trim();
  const password = String(body.password ?? "");
  const confirmPassword = String(body.confirmPassword ?? "");
  const referenceCode = String(body.referenceCode ?? "").trim();
  const otp = String(body.otp ?? "").trim();
  const rateLimit = assertRateLimit({
    key: getRequestFingerprint(request, "auth-register", phone),
    windowMs: 10 * 60 * 1000,
    max: 10
  });

  if (!rateLimit.allowed) {
    return fail(`Too many registration attempts. Try again in ${rateLimit.retryAfterSeconds}s.`, 429, request);
  }

  if (!phone || !password || !confirmPassword || !/^[0-9]{6}$/.test(otp)) {
    return fail("phone, password, confirmPassword, and valid 6 digit OTP are required", 400, request);
  }

  if (password.length < 8) {
    return fail("Password must be at least 8 characters", 400, request);
  }

  if (password !== confirmPassword) {
    return fail("Password and confirm password must match", 400, request);
  }

  const validOtp = await verifyOtp(phone, "register", otp);
  if (!validOtp) {
    return fail("Invalid or expired OTP", 400, request);
  }

  const created = await createUserAccount({
    phone,
    passwordHash: hashCredential(password),
    referenceCode
  });

  if (!created.user) {
    return fail(created.error, 400, request);
  }

  return ok(
    {
      user: {
        id: created.user.id,
        phone: created.user.phone,
        name: created.user.name,
        role: created.user.role,
        referralCode: created.user.referralCode,
        joinedAt: created.user.joinedAt,
        approvalStatus: created.user.approvalStatus
      }
    },
    request
  );
}
