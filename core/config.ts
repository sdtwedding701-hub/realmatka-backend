function readEnv(name: string, fallback = "") {
  return process.env[name]?.trim() || fallback;
}

function readCsvEnv(name: string) {
  return readEnv(name)
    .split(",")
    .map((value) => value.trim())
    .filter(Boolean);
}

function readNumberEnv(name: string, fallback: number) {
  const raw = Number(readEnv(name));
  return Number.isFinite(raw) && raw > 0 ? raw : fallback;
}

const databaseUrl = readEnv("DATABASE_URL");
const databaseProvider = readEnv("DATABASE_PROVIDER", databaseUrl ? "postgres" : "sqlite");
const appUrl = readEnv("EXPO_PUBLIC_APP_URL", "http://localhost:8081");
const apiUrl = readEnv("EXPO_PUBLIC_API_BASE_URL", "");
const adminDomain = readEnv("ADMIN_DOMAIN", "http://localhost:5500");
const extraCorsOrigins = readCsvEnv("EXTRA_CORS_ORIGINS");
const allowedOrigins = [...new Set([appUrl, apiUrl, adminDomain, ...extraCorsOrigins].filter(Boolean))];
const nodeEnv = readEnv("NODE_ENV", "development");

export const productionConfig = {
  appUrl,
  apiUrl,
  databaseUrl,
  databaseProvider,
  nodeEnv,
  otpProvider: readEnv("OTP_PROVIDER", "local"),
  twilioAccountSid: readEnv("TWILIO_ACCOUNT_SID"),
  twilioAuthToken: readEnv("TWILIO_AUTH_TOKEN"),
  twilioVerifyServiceSid: readEnv("TWILIO_VERIFY_SERVICE_SID"),
  paymentProvider: readEnv("PAYMENT_PROVIDER", "manual"),
  paymentKeyId: readEnv("PAYMENT_KEY_ID"),
  paymentKeySecret: readEnv("PAYMENT_KEY_SECRET"),
  notificationProvider: readEnv("NOTIFICATION_PROVIDER", "noop"),
  fcmServerKey: readEnv("FCM_SERVER_KEY"),
  adminDomain,
  allowedOrigins,
  sessionTtlHours: readNumberEnv("SESSION_TTL_HOURS", 24 * 7),
  authRateLimitWindowMs: readNumberEnv("AUTH_RATE_LIMIT_WINDOW_MS", 10 * 60 * 1000),
  authRateLimitMax: readNumberEnv("AUTH_RATE_LIMIT_MAX", 10),
  otpRateLimitWindowMs: readNumberEnv("OTP_RATE_LIMIT_WINDOW_MS", 10 * 60 * 1000),
  otpRateLimitMax: readNumberEnv("OTP_RATE_LIMIT_MAX", 5)
} as const;

if (nodeEnv === "production") {
  const secureUrls = [appUrl, apiUrl, adminDomain].filter(Boolean);
  const insecureUrls = secureUrls.filter((value) => !value.startsWith("https://"));

  if (productionConfig.databaseProvider !== "postgres") {
    throw new Error("Production requires DATABASE_PROVIDER=postgres.");
  }

  if (insecureUrls.length > 0) {
    throw new Error(`Production requires HTTPS URLs. Invalid values: ${insecureUrls.join(", ")}`);
  }
}

export function isProductionReadyConfig() {
  return Boolean(
    productionConfig.databaseUrl &&
      productionConfig.paymentProvider !== "manual" &&
      productionConfig.notificationProvider !== "noop"
  );
}

export function isPostgresEnabled() {
  return productionConfig.databaseProvider === "postgres" && Boolean(productionConfig.databaseUrl);
}
