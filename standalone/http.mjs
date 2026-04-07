import { createHash } from "node:crypto";

function getAllowedOrigin(request) {
  const origin = request.headers.get("origin") || "";
  const allowed = new Set(
    [process.env.EXPO_PUBLIC_APP_URL, process.env.EXPO_PUBLIC_API_BASE_URL, process.env.ADMIN_DOMAIN]
      .filter(Boolean)
      .map((value) => value.replace(/\/$/, ""))
  );

  if (!origin) {
    return "*";
  }

  return allowed.size === 0 || allowed.has(origin.replace(/\/$/, "")) ? origin : "";
}

function getCorsHeaders(request) {
  const origin = getAllowedOrigin(request);
  return {
    ...(origin ? { "Access-Control-Allow-Origin": origin } : {}),
    "Access-Control-Allow-Headers": "Content-Type, Authorization, X-Session-Token",
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
    Vary: "Origin"
  };
}

export async function getJsonBody(request) {
  try {
    return await request.json();
  } catch {
    return {};
  }
}

export function json(data, status = 200, request) {
  return Response.json(data, {
    status,
    headers: getCorsHeaders(request)
  });
}

export function ok(data, request) {
  return json({ ok: true, data }, 200, request);
}

export function fail(error, status = 400, request) {
  return json({ ok: false, error }, status, request);
}

export function unauthorized(request, message = "Unauthorized") {
  return fail(message, 401, request);
}

export function corsPreflight(request) {
  return new Response(null, {
    status: 204,
    headers: getCorsHeaders(request)
  });
}

export function getSessionToken(request) {
  return request.headers.get("authorization")?.replace(/^Bearer\s+/i, "") ?? request.headers.get("x-session-token") ?? "";
}

export function normalizeIndianPhone(input) {
  const digits = String(input ?? "").replace(/\D/g, "");
  if (digits.length === 10) {
    return digits;
  }
  if (digits.length === 12 && digits.startsWith("91")) {
    return digits.slice(2);
  }
  return null;
}

export function hashSecret(value) {
  return createHash("sha256").update(value).digest("hex");
}
