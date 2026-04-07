const readEnv = (name, fallback = "") => {
  const value = process.env[name];
  return typeof value === "string" && value.trim() ? value.trim() : fallback;
};

export const standaloneConfig = {
  port: Number(process.env.PORT || 3000),
  databaseProvider: readEnv("DATABASE_PROVIDER", "sqlite"),
  databaseUrl: readEnv("DATABASE_URL"),
  sessionTtlHours: Number(readEnv("SESSION_TTL_HOURS", "168")),
  appUrl: readEnv("EXPO_PUBLIC_APP_URL", "http://localhost:8081"),
  apiUrl: readEnv("EXPO_PUBLIC_API_BASE_URL", "http://localhost:3000"),
  adminDomain: readEnv("ADMIN_DOMAIN", "http://localhost:5500")
};

export function isStandalonePostgresEnabled() {
  return standaloneConfig.databaseProvider === "postgres" && Boolean(standaloneConfig.databaseUrl);
}
