import { User } from "@/services/backend-service/core/schema";
import { productionConfig } from "@/services/backend-service/core/config";
import { requireUser } from "@/services/backend-service/core/store";

function getCorsHeaders(request?: Request) {
  const requestOrigin = request?.headers.get("origin") ?? "";
  const allowedOrigins = productionConfig.allowedOrigins;
  const allowAnyOrigin = allowedOrigins.length === 0;
  const origin = !requestOrigin ? (allowAnyOrigin ? "*" : "") : allowAnyOrigin || allowedOrigins.includes(requestOrigin) ? requestOrigin : "";

  return {
    ...(origin ? { "Access-Control-Allow-Origin": origin } : {}),
    "Access-Control-Allow-Headers": "Content-Type, Authorization, X-Session-Token",
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
    Vary: "Origin"
  };
}

export async function getJsonBody(request: Request) {
  try {
    return await request.json();
  } catch {
    return {};
  }
}

export function ok(data: unknown, init?: ResponseInit, request?: Request) {
  return Response.json(
    { ok: true, data },
    { status: 200, headers: getCorsHeaders(request), ...init }
  );
}

export function fail(message: string, status = 400, request?: Request) {
  return Response.json(
    { ok: false, error: message },
    { status, headers: getCorsHeaders(request) }
  );
}

export function unauthorized(message = "Unauthorized", request?: Request) {
  return fail(message, 401, request);
}

export function corsPreflight(request?: Request) {
  return new Response(null, {
    status: 204,
    headers: getCorsHeaders(request)
  });
}

export function getSessionToken(request: Request) {
  return request.headers.get("authorization")?.replace("Bearer ", "") ?? request.headers.get("x-session-token");
}

export async function getAuthedUser(request: Request): Promise<User | null> {
  return requireUser(getSessionToken(request));
}

export async function requireAdmin(request: Request) {
  const user = await getAuthedUser(request);
  if (!user) {
    return { user: null, response: unauthorized("Unauthorized", request) };
  }
  if (user.role !== "admin") {
    return { user: null, response: fail("Admin access required", 403, request) };
  }
  return { user, response: null };
}

