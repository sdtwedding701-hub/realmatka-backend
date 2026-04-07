import { createServer } from "node:http";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";
import { Readable } from "node:stream";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const require = createRequire(import.meta.url);
const backendRoot = __dirname;
const workspaceRoot = path.resolve(backendRoot, "..");

async function loadEnvFile(filePath, { override = false } = {}) {
  try {
    const raw = await readFile(filePath, "utf8");
    for (const line of raw.split(/\r?\n/)) {
      const trimmed = line.trim();
      if (!trimmed || trimmed.startsWith("#")) {
        continue;
      }

      const separatorIndex = trimmed.indexOf("=");
      if (separatorIndex <= 0) {
        continue;
      }

      const key = trimmed.slice(0, separatorIndex).trim();
      if (!key || (!override && process.env[key] != null)) {
        continue;
      }

      const value = trimmed.slice(separatorIndex + 1).trim().replace(/^['"]|['"]$/g, "");
      process.env[key] = value;
    }
  } catch (error) {
    if (error?.code !== "ENOENT") {
      throw error;
    }
  }
}

await loadEnvFile(path.join(backendRoot, ".env.production"));
await loadEnvFile(path.join(backendRoot, ".env.local"));
await loadEnvFile(path.join(workspaceRoot, ".env.production"));
await loadEnvFile(path.join(workspaceRoot, ".env.local"));
await loadEnvFile(path.join(workspaceRoot, ".env.backend.local"), { override: true });

const distServerDir = path.resolve(backendRoot, process.env.EXPO_SERVER_DIST_DIR || "dist/server");
const routesManifestPath = path.join(distServerDir, "_expo", "routes.json");
const port = Number(process.env.PORT || 3000);
const configuredCorsOrigins = [
  process.env.EXPO_PUBLIC_APP_URL,
  process.env.ADMIN_DOMAIN,
  process.env.PUBLIC_API_ORIGIN,
  "http://localhost:8085",
  "http://localhost:8083",
  "http://localhost:8082",
  "http://localhost:8081",
  "http://localhost:5501",
  "http://localhost:5500",
  "http://127.0.0.1:8085",
  "http://127.0.0.1:8083",
  "http://127.0.0.1:8082",
  "http://127.0.0.1:8081",
  "http://127.0.0.1:5501",
  "http://127.0.0.1:5500"
]
  .flatMap((value) => (value ? value.split(",") : []))
  .map((value) => value.trim().replace(/\/$/, ""))
  .filter(Boolean);
const allowedCorsOrigins = new Set(configuredCorsOrigins);

function isAllowedCorsOrigin(origin) {
  if (!origin) {
    return false;
  }

  if (allowedCorsOrigins.has(origin)) {
    return true;
  }

  return /^(https?:\/\/)(localhost|127\.0\.0\.1|192\.168\.\d+\.\d+|10\.\d+\.\d+\.\d+)(:\d+)?$/i.test(origin);
}
const authRoutes = await import("./standalone/routes/auth.mjs");
const authAccountRoutes = await import("./standalone/routes/auth-account.mjs");
const authOtpRoutes = await import("./standalone/routes/auth-otp.mjs");
const authRegisterRoutes = await import("./standalone/routes/auth-register.mjs");
const walletRoutes = await import("./standalone/routes/wallet.mjs");
const walletBalanceRoutes = await import("./standalone/routes/wallet-balance.mjs");
const bidsRoutes = await import("./standalone/routes/bids.mjs");
const bankRoutes = await import("./standalone/routes/bank.mjs");
const profileRoutes = await import("./standalone/routes/profile.mjs");
const notificationsRoutes = await import("./standalone/routes/notifications.mjs");
const paymentsRoutes = await import("./standalone/routes/payments.mjs");
const marketsRoutes = await import("./standalone/routes/markets.mjs");
const bidsPlaceRoutes = await import("./standalone/routes/bids-place.mjs");
const adminRoutes = await import("./standalone/routes/admin.mjs");
const standaloneRoutes = new Map([
  ["/api/auth/login", { OPTIONS: authRoutes.options, POST: authRoutes.login }],
  ["/api/auth/me", { OPTIONS: authRoutes.options, GET: authRoutes.me }],
  ["/api/auth/request-otp", { OPTIONS: authOtpRoutes.options, POST: authOtpRoutes.requestOtp }],
  ["/api/auth/otp-login", { OPTIONS: authOtpRoutes.options, POST: authOtpRoutes.otpLogin }],
  ["/api/auth/forgot-password", { OPTIONS: authOtpRoutes.options, POST: authOtpRoutes.forgotPassword }],
  ["/api/auth/register", { OPTIONS: authRegisterRoutes.options, POST: authRegisterRoutes.register }],
  ["/api/auth/logout", { OPTIONS: authAccountRoutes.options, POST: authAccountRoutes.logout }],
  ["/api/auth/update-password", { OPTIONS: authAccountRoutes.options, POST: authAccountRoutes.updatePassword }],
  ["/api/auth/update-mpin", { OPTIONS: authAccountRoutes.options, POST: authAccountRoutes.updateMpin }],
  ["/api/profile/update", { OPTIONS: profileRoutes.options, POST: profileRoutes.update }],
  ["/api/wallet/balance", { OPTIONS: walletBalanceRoutes.options, GET: walletBalanceRoutes.balance }],
  ["/api/wallet/history", { OPTIONS: walletRoutes.options, GET: walletRoutes.history }],
  ["/api/wallet/deposit", { OPTIONS: walletRoutes.options, POST: walletRoutes.deposit }],
  ["/api/wallet/withdraw", { OPTIONS: walletRoutes.options, POST: walletRoutes.withdraw }],
  ["/api/bids/history", { OPTIONS: bidsRoutes.options, GET: bidsRoutes.history }],
  ["/api/bids/place", { OPTIONS: bidsPlaceRoutes.options, POST: bidsPlaceRoutes.place }],
  ["/api/bids/board-helper", { OPTIONS: bidsPlaceRoutes.options, GET: bidsPlaceRoutes.boardHelper }],
  ["/api/bank/list", { OPTIONS: bankRoutes.options, GET: bankRoutes.list }],
  ["/api/bank/add", { OPTIONS: bankRoutes.options, POST: bankRoutes.add }],
  ["/api/markets/list", { OPTIONS: marketsRoutes.options, GET: marketsRoutes.list }],
  ["/api/notifications/history", { OPTIONS: notificationsRoutes.options, GET: notificationsRoutes.history }],
  ["/api/notifications/devices/register", { OPTIONS: notificationsRoutes.options, POST: notificationsRoutes.registerDevice }],
  ["/api/payments/create-order", { OPTIONS: paymentsRoutes.options, POST: paymentsRoutes.createOrder }],
  ["/api/payments/webhook", { OPTIONS: paymentsRoutes.options, POST: paymentsRoutes.webhook }],
  ["/api/settings", { OPTIONS: adminRoutes.options, GET: adminRoutes.settingsPublic }],
  ["/api/admin/users", { OPTIONS: adminRoutes.options, GET: adminRoutes.users }],
  ["/api/admin/user-detail", { OPTIONS: adminRoutes.options, GET: adminRoutes.userDetail }],
  ["/api/admin/user-approval", { OPTIONS: adminRoutes.options, POST: adminRoutes.userApproval }],
  ["/api/admin/user-status", { OPTIONS: adminRoutes.options, POST: adminRoutes.userStatus }],
  ["/api/admin/wallet-requests", { OPTIONS: adminRoutes.options, GET: adminRoutes.walletRequests }],
  ["/api/admin/wallet-request-history", { OPTIONS: adminRoutes.options, GET: adminRoutes.walletRequestHistory }],
  ["/api/admin/wallet-request-action", { OPTIONS: adminRoutes.options, POST: adminRoutes.walletRequestAction }],
  ["/api/admin/wallet-adjustment", { OPTIONS: adminRoutes.options, POST: adminRoutes.walletAdjustment }],
  ["/api/admin/audit-logs", { OPTIONS: adminRoutes.options, GET: adminRoutes.auditLogs }],
  ["/api/admin/bids", { OPTIONS: adminRoutes.options, GET: adminRoutes.bidsList }],
  ["/api/admin/notifications", { OPTIONS: adminRoutes.options, GET: adminRoutes.notificationsList, POST: adminRoutes.notificationsSend }],
  ["/api/admin/settings", { OPTIONS: adminRoutes.options, GET: adminRoutes.settingsGet, POST: adminRoutes.settingsUpdate }],
  ["/api/admin/chart-update", { OPTIONS: adminRoutes.options, POST: adminRoutes.chartUpdate }],
  ["/api/admin/market-update", { OPTIONS: adminRoutes.options, POST: adminRoutes.marketUpdate }],
  ["/api/admin/settle-market", { OPTIONS: adminRoutes.options, POST: adminRoutes.settleMarket }],
  ["/api/admin/settlement-preview", { OPTIONS: adminRoutes.options, GET: adminRoutes.settlementPreview }],
  ["/api/admin/reconciliation-summary", { OPTIONS: adminRoutes.options, GET: adminRoutes.reconciliationSummary }],
  ["/api/admin/monitoring-summary", { OPTIONS: adminRoutes.options, GET: adminRoutes.monitoringSummary }],
  ["/api/admin/export", { OPTIONS: adminRoutes.options, GET: adminRoutes.exportData }],
  ["/api/admin/backup-snapshot", { OPTIONS: adminRoutes.options, GET: adminRoutes.backupSnapshot, POST: adminRoutes.restoreSnapshot }],
  ["/api/admin/dashboard-summary", { OPTIONS: adminRoutes.options, GET: adminRoutes.dashboardSummary }],
  ["/api/admin/reports-summary", { OPTIONS: adminRoutes.options, GET: adminRoutes.reportsSummary }]
]);

let cachedManifest = null;
const handlerCache = new Map();

async function loadManifest() {
  if (cachedManifest) {
    return cachedManifest;
  }

  const rawManifest = await readFile(routesManifestPath, "utf8");
  const parsed = JSON.parse(rawManifest);

  cachedManifest = {
    apiRoutes: (parsed.apiRoutes || []).map((route) => ({
      ...route,
      matcher: new RegExp(route.namedRegex)
    }))
  };

  return cachedManifest;
}

function getRouteMatch(pathname, routes) {
  for (const route of routes) {
    const match = pathname.match(route.matcher);
    if (match) {
      return {
        route,
        params: match.groups || {}
      };
    }
  }

  return null;
}

async function loadRouteModule(routeFile) {
  if (handlerCache.has(routeFile)) {
    return handlerCache.get(routeFile);
  }

  const modulePath = path.join(distServerDir, routeFile);
  const imported = require(modulePath);
  const resolved =
    imported?.default && typeof imported.default === "object"
      ? imported.default
      : imported;

  handlerCache.set(routeFile, resolved);
  return resolved;
}

function toWebRequest(req) {
  const origin = process.env.PUBLIC_API_ORIGIN || `http://${req.headers.host || `localhost:${port}`}`;
  const url = new URL(req.url || "/", origin);
  const method = req.method || "GET";
  const headers = new Headers();

  for (const [key, value] of Object.entries(req.headers)) {
    if (Array.isArray(value)) {
      for (const item of value) {
        headers.append(key, item);
      }
    } else if (typeof value === "string") {
      headers.set(key, value);
    }
  }

  const init = {
    method,
    headers,
    body: method === "GET" || method === "HEAD" ? undefined : Readable.toWeb(req),
    duplex: method === "GET" || method === "HEAD" ? undefined : "half"
  };

  return new Request(url, init);
}

async function sendWebResponse(nodeRes, webResponse) {
  nodeRes.statusCode = webResponse.status;
  nodeRes.statusMessage = webResponse.statusText;

  webResponse.headers.forEach((value, key) => {
    nodeRes.setHeader(key, value);
  });

  if (!webResponse.body) {
    nodeRes.end();
    return;
  }

  const body = Readable.fromWeb(webResponse.body);
  body.on("error", (error) => {
    console.error("Response stream error", error);
    if (!nodeRes.headersSent) {
      nodeRes.statusCode = 500;
    }
    nodeRes.end();
  });
  body.pipe(nodeRes);
}

function sendJson(nodeRes, statusCode, payload) {
  const body = JSON.stringify(payload);
  nodeRes.statusCode = statusCode;
  nodeRes.setHeader("content-type", "application/json; charset=utf-8");
  nodeRes.end(body);
}

function applyCorsHeaders(req, res) {
  const requestOrigin = typeof req.headers.origin === "string" ? req.headers.origin.replace(/\/$/, "") : "";

  if (isAllowedCorsOrigin(requestOrigin)) {
    res.setHeader("access-control-allow-origin", requestOrigin);
    res.setHeader("vary", "Origin");
  }

  res.setHeader("access-control-allow-methods", "GET, POST, OPTIONS");
  res.setHeader("access-control-allow-headers", "Content-Type, Authorization");
}

async function handleApiRequest(req, res) {
  const pathname = new URL(req.url || "/", "http://localhost").pathname;
  const methodName = (req.method || "GET").toUpperCase();
  const standaloneRoute = standaloneRoutes.get(pathname);

  if (standaloneRoute) {
    const handler = standaloneRoute[methodName];
    if (typeof handler !== "function") {
      res.setHeader("allow", Object.keys(standaloneRoute).join(", "));
      sendJson(res, 405, { ok: false, error: "Method not allowed" });
      return;
    }

    const webRequest = toWebRequest(req);
    const webResponse = await handler(webRequest);
    await sendWebResponse(res, webResponse);
    return;
  }

  if (pathname.startsWith("/api/markets/")) {
    const slug = pathname.replace(/^\/api\/markets\//, "").replace(/\/$/, "");
    if (slug) {
      const handler = methodName === "OPTIONS" ? marketsRoutes.options : methodName === "GET" ? marketsRoutes.detail : null;
      if (typeof handler !== "function") {
        res.setHeader("allow", "GET, OPTIONS");
        sendJson(res, 405, { ok: false, error: "Method not allowed" });
        return;
      }
      const webRequest = toWebRequest(req);
      const webResponse = await handler(webRequest, { slug });
      await sendWebResponse(res, webResponse);
      return;
    }
  }

  if (pathname.startsWith("/api/charts/")) {
    const slug = pathname.replace(/^\/api\/charts\//, "").replace(/\/$/, "");
    if (slug) {
      const handler = methodName === "OPTIONS" ? marketsRoutes.options : methodName === "GET" ? marketsRoutes.chart : null;
      if (typeof handler !== "function") {
        res.setHeader("allow", "GET, OPTIONS");
        sendJson(res, 405, { ok: false, error: "Method not allowed" });
        return;
      }
      const webRequest = toWebRequest(req);
      const webResponse = await handler(webRequest, { slug });
      await sendWebResponse(res, webResponse);
      return;
    }
  }

  const { apiRoutes } = await loadManifest();
  const match = getRouteMatch(pathname, apiRoutes);

  if (!match) {
    sendJson(res, 404, { ok: false, error: "Route not found" });
    return;
  }

  const routeModule = await loadRouteModule(match.route.file);
  const handler = routeModule[methodName];

  if (typeof handler !== "function") {
    res.setHeader("allow", Object.keys(routeModule).filter((key) => /^[A-Z]+$/.test(key)).join(", "));
    sendJson(res, 405, { ok: false, error: "Method not allowed" });
    return;
  }

  const webRequest = toWebRequest(req);
  const webResponse = await handler(webRequest, match.params);
  await sendWebResponse(res, webResponse);
}

const server = createServer(async (req, res) => {
  try {
    const pathname = new URL(req.url || "/", "http://localhost").pathname;
    applyCorsHeaders(req, res);

    if ((req.method || "GET").toUpperCase() === "OPTIONS" && pathname.startsWith("/api/")) {
      res.statusCode = 204;
      res.end();
      return;
    }

    if (pathname === "/health") {
      sendJson(res, 200, {
        ok: true,
        service: "realmatka-api",
        timestamp: new Date().toISOString()
      });
      return;
    }

    if (pathname.startsWith("/api/")) {
      await handleApiRequest(req, res);
      return;
    }

    sendJson(res, 200, {
      ok: true,
      service: "realmatka-api",
      routes: ["/health", "/api/*"]
    });
  } catch (error) {
    console.error("Unhandled backend request error", error);
    sendJson(res, 500, {
      ok: false,
      error: error instanceof Error ? error.message : "Internal server error"
    });
  }
});

server.listen(port, () => {
  console.log(`Real Matka backend listening on port ${port}`);
});
