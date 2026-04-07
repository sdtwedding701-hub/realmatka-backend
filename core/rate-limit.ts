type RateLimitOptions = {
  key: string;
  windowMs: number;
  max: number;
};

type RateLimitState = {
  count: number;
  resetAt: number;
};

const rateLimitStore = new Map<string, RateLimitState>();

export function getRequestFingerprint(request: Request, scope: string, identifier = "") {
  const forwardedFor = request.headers.get("x-forwarded-for")?.split(",")[0]?.trim() ?? "";
  const realIp = request.headers.get("x-real-ip")?.trim() ?? "";
  const agent = request.headers.get("user-agent")?.trim() ?? "";
  return [scope, identifier, forwardedFor || realIp || "local", agent.slice(0, 80)].join(":");
}

export function assertRateLimit(options: RateLimitOptions) {
  const now = Date.now();
  const current = rateLimitStore.get(options.key);

  if (!current || current.resetAt <= now) {
    rateLimitStore.set(options.key, { count: 1, resetAt: now + options.windowMs });
    return { allowed: true, retryAfterSeconds: 0 };
  }

  if (current.count >= options.max) {
    return {
      allowed: false,
      retryAfterSeconds: Math.max(1, Math.ceil((current.resetAt - now) / 1000))
    };
  }

  current.count += 1;
  rateLimitStore.set(options.key, current);
  return { allowed: true, retryAfterSeconds: 0 };
}
