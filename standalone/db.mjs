import { randomBytes, scryptSync, timingSafeEqual } from "node:crypto";
import { mkdirSync, readFileSync } from "node:fs";
import path from "node:path";
import { DatabaseSync } from "node:sqlite";
import { Pool } from "pg";
import { fileURLToPath } from "node:url";
import { standaloneConfig, isStandalonePostgresEnabled } from "./config.mjs";
import { hashSecret } from "./http.mjs";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const backendRoot = path.resolve(__dirname, "..");
const sqlitePath = path.join(backendRoot, "data", "server.db");
const postgresSchemaSql = readFileSync(path.join(backendRoot, "postgres-schema.sql"), "utf8");
const sessionTtlMs = standaloneConfig.sessionTtlHours * 60 * 60 * 1000;
const signupBonusAmount = 25;

let sqlite = null;
let pgPool = null;
let pgBootstrapPromise = null;

const defaultUser = {
  id: "user_1",
  phone: "9309782081",
  passwordHash: hashSecret("demo1234"),
  mpinHash: hashSecret("1234"),
  name: "Siddhant Borkar",
  joinedAt: "2025-04-12T10:00:00.000Z",
  referralCode: "621356",
  role: "admin",
  approvalStatus: "Approved"
};

const defaultWalletEntry = {
  id: "wallet_1",
  userId: defaultUser.id,
  type: "DEPOSIT",
  status: "SUCCESS",
  amount: 0,
  beforeBalance: 0,
  afterBalance: 0
};

const seededMarkets = [
  ["seed_ntr_morning", "ntr-morning", "NTR Morning", "***-**-***", "Betting open now", "Place Bet", "09:00 AM", "10:00 AM", "main"],
  ["seed_sita_morning", "sita-morning", "Sita Morning", "***-**-***", "Betting open now", "Place Bet", "09:40 AM", "10:40 AM", "main"],
  ["seed_karnataka_day", "karnataka-day", "Karnataka Day", "***-**-***", "Betting open now", "Place Bet", "09:55 AM", "10:55 AM", "main"],
  ["seed_star_tara_morning", "star-tara-morning", "Star Tara Morning", "***-**-***", "Betting open now", "Place Bet", "10:05 AM", "11:05 AM", "main"],
  ["seed_milan_morning", "milan-morning", "Milan Morning", "***-**-***", "Betting open now", "Place Bet", "10:10 AM", "11:10 AM", "main"],
  ["seed_maya_bazar", "maya-bazar", "Maya Bazar", "***-**-***", "Betting open now", "Place Bet", "10:15 AM", "11:15 AM", "main"],
  ["seed_andhra_morning", "andhra-morning", "Andhra Morning", "***-**-***", "Betting open now", "Place Bet", "10:35 AM", "11:35 AM", "main"],
  ["seed_sridevi", "sridevi", "Sridevi", "***-**-***", "Betting open now", "Place Bet", "11:25 AM", "12:25 PM", "main"],
  ["seed_mahadevi_morning", "mahadevi-morning", "Mahadevi Morning", "***-**-***", "Betting open now", "Place Bet", "11:40 AM", "12:40 PM", "main"],
  ["seed_time_bazar", "time-bazar", "Time Bazar", "***-**-***", "Betting open now", "Place Bet", "12:45 PM", "01:45 PM", "main"],
  ["seed_madhur_day", "madhur-day", "Madhur Day", "***-**-***", "Betting open now", "Place Bet", "01:20 PM", "02:20 PM", "main"],
  ["seed_sita_day", "sita-day", "Sita Day", "***-**-***", "Betting open now", "Place Bet", "01:40 PM", "02:40 PM", "main"],
  ["seed_star_tara_day", "star-tara-day", "Star Tara Day", "***-**-***", "Betting open now", "Place Bet", "02:15 PM", "03:15 PM", "main"],
  ["seed_ntr_bazar", "ntr-bazar", "NTR Bazar", "***-**-***", "Betting open now", "Place Bet", "02:45 PM", "03:50 PM", "main"],
  ["seed_milan_day", "milan-day", "Milan Day", "***-**-***", "Betting open now", "Place Bet", "02:45 PM", "04:45 PM", "main"],
  ["seed_rajdhani_day", "rajdhani-day", "Rajdhani Day", "***-**-***", "Betting open now", "Place Bet", "03:00 PM", "05:00 PM", "main"],
  ["seed_andhra_day", "andhra-day", "Andhra Day", "***-**-***", "Betting open now", "Place Bet", "03:30 PM", "05:30 PM", "main"],
  ["seed_kalyan", "kalyan", "Kalyan", "***-**-***", "Betting open now", "Place Bet", "04:10 PM", "06:10 PM", "main"],
  ["seed_mahadevi", "mahadevi", "Mahadevi", "***-**-***", "Betting open now", "Place Bet", "04:25 PM", "06:25 PM", "main"],
  ["seed_ntr_day", "ntr-day", "NTR Day", "***-**-***", "Betting open now", "Place Bet", "04:50 PM", "06:50 PM", "main"],
  ["seed_sita_night", "sita-night", "Sita Night", "***-**-***", "Betting open now", "Place Bet", "06:40 PM", "07:40 PM", "main"],
  ["seed_sridevi_night", "sridevi-night", "Sridevi Night", "***-**-***", "Betting open now", "Place Bet", "07:05 PM", "08:05 PM", "main"],
  ["seed_star_tara_night", "star-tara-night", "Star Tara Night", "***-**-***", "Betting open now", "Place Bet", "07:15 PM", "08:15 PM", "main"],
  ["seed_mahadevi_night", "mahadevi-night", "Mahadevi Night", "***-**-***", "Betting open now", "Place Bet", "07:45 PM", "08:45 PM", "main"],
  ["seed_madhur_night", "madhur-night", "Madhur Night", "***-**-***", "Betting open now", "Place Bet", "08:20 PM", "10:20 PM", "main"],
  ["seed_supreme_night", "supreme-night", "Supreme Night", "***-**-***", "Betting open now", "Place Bet", "08:35 PM", "10:35 PM", "main"],
  ["seed_andhra_night", "andhra-night", "Andhra Night", "***-**-***", "Betting open now", "Place Bet", "08:40 PM", "10:40 PM", "main"],
  ["seed_ntr_night", "ntr-night", "NTR Night", "***-**-***", "Betting open now", "Place Bet", "08:50 PM", "10:50 PM", "main"],
  ["seed_milan_night", "milan-night", "Milan Night", "***-**-***", "Betting open now", "Place Bet", "08:50 PM", "10:50 PM", "main"],
  ["seed_kalyan_night", "kalyan-night", "Kalyan Night", "***-**-***", "Betting open now", "Place Bet", "09:25 PM", "11:25 PM", "main"],
  ["seed_rajdhani_night", "rajdhani-night", "Rajdhani Night", "***-**-***", "Betting open now", "Place Bet", "09:30 PM", "11:40 PM", "main"],
  ["seed_main_bazar", "main-bazar", "Main Bazar", "***-**-***", "Betting open now", "Place Bet", "09:45 PM", "11:55 PM", "main"],
  ["seed_mangal_bazar", "mangal-bazar", "Mangal Bazar", "***-**-***", "Betting open now", "Place Bet", "10:05 PM", "11:05 PM", "main"]
];

function nowIso() {
  return new Date().toISOString();
}

function toIso(value) {
  if (!value) {
    return null;
  }
  return value instanceof Date ? value.toISOString() : String(value);
}

function toBool(value) {
  return value === true || value === 1 || value === "1" || value === "true";
}

function toChartRows(value) {
  if (Array.isArray(value)) {
    return value;
  }
  if (typeof value === "string" && value.trim()) {
    return JSON.parse(value);
  }
  return [];
}

function formatChartDayForRows(value) {
  const month = value.toLocaleString("en-US", { month: "short" });
  const day = String(value.getDate()).padStart(2, "0");
  return `${month} ${day}`;
}

function getWeekStartForRows(date) {
  const value = new Date(date);
  value.setHours(0, 0, 0, 0);
  const day = value.getDay();
  const diff = day === 0 ? -6 : 1 - day;
  value.setDate(value.getDate() + diff);
  return value;
}

function getWeekEndForRows(date) {
  const value = getWeekStartForRows(date);
  value.setDate(value.getDate() + 6);
  return value;
}

function getWeekChartLabelForRows(date) {
  const start = getWeekStartForRows(date);
  const end = getWeekEndForRows(date);
  return `${start.getFullYear()} ${formatChartDayForRows(start)} to ${formatChartDayForRows(end)}`;
}

function parseWeekLabelStartDateForRows(label) {
  const value = String(label || "").trim();
  let match = value.match(/^(\d{4})\s+([A-Za-z]{3})\s+(\d{2})\s+to\s+([A-Za-z]{3})\s+(\d{2})$/);
  if (match) {
    const [, year, month, day] = match;
    const parsed = new Date(`${month} ${day}, ${year} 00:00:00`);
    return Number.isNaN(parsed.getTime()) ? null : parsed;
  }

  match = value.match(/^(\d{4})\s+(\d{2})\s+([A-Za-z]{3})\s+to\s+(\d{2})\s+([A-Za-z]{3})$/);
  if (match) {
    const [, year, day, month] = match;
    const parsed = new Date(`${month} ${day}, ${year} 00:00:00`);
    return Number.isNaN(parsed.getTime()) ? null : parsed;
  }

  return null;
}

function normalizeWeekLabelForRows(label) {
  const parsed = parseWeekLabelStartDateForRows(label);
  return parsed ? getWeekChartLabelForRows(parsed) : String(label || "").trim();
}

function isPlaceholderChartCellForRows(value) {
  const text = String(value || "").trim();
  return !text || text === "**" || text === "***" || text === "--" || text === "---";
}

function sortChartRowsChronologicallyForRows(rows) {
  return [...rows].sort((left, right) => {
    const leftParsed = parseWeekLabelStartDateForRows(left?.[0]);
    const rightParsed = parseWeekLabelStartDateForRows(right?.[0]);
    const leftTime = leftParsed ? leftParsed.getTime() : Number.MAX_SAFE_INTEGER;
    const rightTime = rightParsed ? rightParsed.getTime() : Number.MAX_SAFE_INTEGER;
    return leftTime - rightTime;
  });
}

function normalizeChartRowsForStorage(chartType, rows) {
  const size = chartType === "panna" ? 14 : 7;
  const placeholder = chartType === "panna" ? "---" : "--";
  const merged = new Map();

  for (const sourceRow of Array.isArray(rows) ? rows : []) {
    if (!Array.isArray(sourceRow) || sourceRow.length === 0) {
      continue;
    }

    const label = normalizeWeekLabelForRows(sourceRow[0]);
    const base = merged.get(label) ?? [label, ...Array.from({ length: size }, () => placeholder)];
    for (let index = 0; index < size; index += 1) {
      const candidate = String(sourceRow[index + 1] ?? "").trim();
      if (!isPlaceholderChartCellForRows(candidate)) {
        base[index + 1] = candidate;
      }
    }
    merged.set(label, base);
  }

  return sortChartRowsChronologicallyForRows(Array.from(merged.values()));
}

function mapUserRow(row) {
  return row
    ? {
        id: row.id,
        phone: row.phone,
        passwordHash: row.password_hash,
        mpinHash: row.mpin_hash,
        name: row.name,
        joinedAt: toIso(row.joined_at),
        referralCode: row.referral_code,
        role: row.role,
        approvalStatus: row.approval_status ?? "Approved",
        approvedAt: toIso(row.approved_at),
        rejectedAt: toIso(row.rejected_at),
        blockedAt: toIso(row.blocked_at),
        deactivatedAt: toIso(row.deactivated_at),
        statusNote: row.status_note ?? "",
        signupBonusGranted: toBool(row.signup_bonus_granted),
        referredByUserId: row.referred_by_user_id ?? null
      }
    : null;
}

function mapWalletEntryRow(row) {
  return row
    ? {
        id: row.id,
        userId: row.user_id,
        type: row.type,
        status: row.status,
        amount: Number(row.amount),
        beforeBalance: Number(row.before_balance),
        afterBalance: Number(row.after_balance),
        referenceId: row.reference_id ?? "",
        proofUrl: row.proof_url ?? "",
        note: row.note ?? "",
        createdAt: toIso(row.created_at)
      }
    : null;
}

function mapPaymentOrderRow(row) {
  return row
    ? {
        id: row.id,
        userId: row.user_id,
        provider: row.provider,
        amount: Number(row.amount),
        status: row.status,
        reference: row.reference,
        checkoutToken: row.checkout_token ?? null,
        gatewayOrderId: row.gateway_order_id ?? null,
        gatewayPaymentId: row.gateway_payment_id ?? null,
        gatewaySignature: row.gateway_signature ?? null,
        verifiedAt: toIso(row.verified_at),
        redirectUrl: row.redirect_url ?? null,
        createdAt: toIso(row.created_at),
        updatedAt: toIso(row.updated_at)
      }
    : null;
}

function mapBidRow(row) {
  return row
    ? {
        id: row.id,
        userId: row.user_id,
        market: row.market,
        boardLabel: row.board_label,
        sessionType: row.session_type,
        digit: row.digit,
        points: Number(row.points),
        status: row.status,
        payout: Number(row.payout ?? 0),
        settledAt: toIso(row.settled_at),
        settledResult: row.settled_result ?? null,
        createdAt: toIso(row.created_at)
      }
    : null;
}

function mapBankRow(row) {
  return row
    ? {
        id: row.id,
        accountNumber: row.account_number,
        holderName: row.holder_name,
        ifsc: row.ifsc,
        createdAt: toIso(row.created_at)
      }
    : null;
}

function mapMarketRow(row) {
  return row
    ? {
        id: row.id,
        slug: row.slug,
        name: row.name,
        result: row.result,
        status: row.status,
        action: row.action,
        open: row.open_time,
        close: row.close_time,
        category: row.category
      }
    : null;
}

function parseClockTimeToMinutes(value) {
  if (typeof value !== "string") {
    return Number.MAX_SAFE_INTEGER;
  }

  const match = value.trim().match(/^(\d{1,2}):(\d{2})\s*([AP]M)$/i);
  if (!match) {
    return Number.MAX_SAFE_INTEGER;
  }

  let hours = Number(match[1]) % 12;
  const minutes = Number(match[2]);
  const meridiem = match[3].toUpperCase();

  if (meridiem === "PM") {
    hours += 12;
  }

  return hours * 60 + minutes;
}

function sortMarketsByOpenTime(markets) {
  return [...markets].sort((left, right) => {
    const openDiff = parseClockTimeToMinutes(left.open) - parseClockTimeToMinutes(right.open);
    if (openDiff !== 0) {
      return openDiff;
    }

    const closeDiff = parseClockTimeToMinutes(left.close) - parseClockTimeToMinutes(right.close);
    if (closeDiff !== 0) {
      return closeDiff;
    }

    return left.name.localeCompare(right.name);
  });
}

function mapNotificationDeviceRow(row) {
  return row
    ? {
        id: row.id,
        userId: row.user_id,
        platform: row.platform,
        token: row.token,
        enabled: toBool(row.enabled),
        createdAt: toIso(row.created_at),
        updatedAt: toIso(row.updated_at)
      }
    : null;
}

function mapAuditLogRow(row) {
  return row
    ? {
        id: row.id,
        actorUserId: row.actor_user_id,
        action: row.action,
        entityType: row.entity_type,
        entityId: row.entity_id,
        details: row.details,
        createdAt: toIso(row.created_at)
      }
    : null;
}

function mapAppSettingRow(row) {
  return row
    ? {
        key: row.setting_key,
        value: row.setting_value,
        updatedAt: toIso(row.updated_at)
      }
    : null;
}

function ensureSqliteColumn(db, tableName, columnName, definition) {
  const columns = db.prepare(`PRAGMA table_info(${tableName})`).all();
  if (!columns.some((column) => column.name === columnName)) {
    db.exec(`ALTER TABLE ${tableName} ADD COLUMN ${columnName} ${definition}`);
  }
}

function verifyCredential(input, storedHash) {
  if (typeof storedHash !== "string" || !storedHash) {
    return false;
  }

  if (storedHash.startsWith("scrypt$")) {
    const [, salt, expected] = storedHash.split("$");
    if (!salt || !expected) {
      return false;
    }

    const actual = Buffer.from(scryptSync(input, salt, 64).toString("hex"));
    const desired = Buffer.from(expected);
    return actual.length === desired.length && timingSafeEqual(actual, desired);
  }

  const actual = Buffer.from(hashSecret(input));
  const desired = Buffer.from(storedHash);
  return actual.length === desired.length && timingSafeEqual(actual, desired);
}

function hashCredential(input) {
  const salt = randomBytes(16).toString("hex");
  const digest = scryptSync(input, salt, 64).toString("hex");
  return `scrypt$${salt}$${digest}`;
}

function isLocalPostgresUrl(rawUrl) {
  try {
    const parsed = new URL(rawUrl);
    const hostname = (parsed.hostname || "").toLowerCase();
    return (
      hostname === "localhost" ||
      hostname === "127.0.0.1" ||
      hostname === "::1" ||
      hostname.startsWith("192.168.") ||
      hostname.startsWith("10.") ||
      /^172\.(1[6-9]|2\d|3[0-1])\./.test(hostname)
    );
  } catch {
    return false;
  }
}

async function ensurePostgresBootstrap(pool) {
  if (pgBootstrapPromise) {
    return pgBootstrapPromise;
  }

  pgBootstrapPromise = (async () => {
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      const usersTableExists = Boolean((await client.query(`SELECT to_regclass('public.users') AS value`)).rows[0]?.value);
      if (!usersTableExists) {
        await client.query(postgresSchemaSql);
      }
      await client.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS blocked_at TIMESTAMPTZ`);
      await client.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS deactivated_at TIMESTAMPTZ`);
      await client.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS status_note TEXT`);
      await client.query(`ALTER TABLE wallet_entries ADD COLUMN IF NOT EXISTS reference_id TEXT`);
      await client.query(`ALTER TABLE wallet_entries ADD COLUMN IF NOT EXISTS proof_url TEXT`);
      await client.query(`ALTER TABLE wallet_entries ADD COLUMN IF NOT EXISTS note TEXT`);
      await client.query(`ALTER TABLE payment_orders ADD COLUMN IF NOT EXISTS checkout_token TEXT`);
      await client.query(`ALTER TABLE payment_orders ADD COLUMN IF NOT EXISTS gateway_order_id TEXT`);
      await client.query(`ALTER TABLE payment_orders ADD COLUMN IF NOT EXISTS gateway_payment_id TEXT`);
      await client.query(`ALTER TABLE payment_orders ADD COLUMN IF NOT EXISTS gateway_signature TEXT`);
      await client.query(`ALTER TABLE payment_orders ADD COLUMN IF NOT EXISTS verified_at TIMESTAMPTZ`);
      await client.query(`
        CREATE TABLE IF NOT EXISTS app_settings (
          setting_key TEXT PRIMARY KEY,
          setting_value TEXT NOT NULL,
          updated_at TIMESTAMPTZ NOT NULL
        )
      `);

      const userCount = Number((await client.query("SELECT COUNT(*)::int AS count FROM users")).rows[0]?.count ?? 0);
      if (userCount === 0) {
        await client.query(
          `INSERT INTO users (id, phone, password_hash, mpin_hash, name, joined_at, referral_code, role, approval_status, approved_at, signup_bonus_granted)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, TRUE)`,
          [
            defaultUser.id,
            defaultUser.phone,
            defaultUser.passwordHash,
            defaultUser.mpinHash,
            defaultUser.name,
            defaultUser.joinedAt,
            defaultUser.referralCode,
            defaultUser.role,
            defaultUser.approvalStatus,
            defaultUser.joinedAt
          ]
        );
      }

      const walletCount = Number((await client.query("SELECT COUNT(*)::int AS count FROM wallet_entries")).rows[0]?.count ?? 0);
      if (walletCount === 0) {
        await client.query(
          `INSERT INTO wallet_entries (id, user_id, type, status, amount, before_balance, after_balance, created_at)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
          [
            defaultWalletEntry.id,
            defaultWalletEntry.userId,
            defaultWalletEntry.type,
            defaultWalletEntry.status,
            defaultWalletEntry.amount,
            defaultWalletEntry.beforeBalance,
            defaultWalletEntry.afterBalance,
            nowIso()
          ]
        );
      }

      for (const market of seededMarkets) {
        await client.query(
          `INSERT INTO markets (id, slug, name, result, status, action, open_time, close_time, category)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
           ON CONFLICT (slug) DO UPDATE SET
             name = EXCLUDED.name,
             result = EXCLUDED.result,
             status = EXCLUDED.status,
             action = EXCLUDED.action,
             open_time = EXCLUDED.open_time,
             close_time = EXCLUDED.close_time,
             category = EXCLUDED.category`,
          market
        );
      }

      const settingsCount = Number((await client.query("SELECT COUNT(*)::int AS count FROM app_settings")).rows[0]?.count ?? 0);
      if (settingsCount === 0) {
        const settings = [
          ["notice_text", "Withdraw approvals aur result updates yahan se control hote hain."],
          ["support_phone", defaultUser.phone],
          ["support_hours", "10:00 AM - 10:00 PM"],
          ["bonus_enabled", "true"],
          ["bonus_text", "Signup bonus aur promo offers ko dashboard se monitor karo."]
        ];
        for (const [key, value] of settings) {
          await client.query(
            `INSERT INTO app_settings (setting_key, setting_value, updated_at) VALUES ($1, $2, $3)`,
            [key, value, nowIso()]
          );
        }
      }

      await client.query("COMMIT");
    } catch (error) {
      await client.query("ROLLBACK");
      pgBootstrapPromise = null;
      throw error;
    } finally {
      client.release();
    }
  })();

  return pgBootstrapPromise;
}

function getSqlite() {
  if (sqlite) {
    return sqlite;
  }

  mkdirSync(path.dirname(sqlitePath), { recursive: true });
  sqlite = new DatabaseSync(sqlitePath);
  sqlite.exec(`
    PRAGMA journal_mode = WAL;

    CREATE TABLE IF NOT EXISTS users (
      id TEXT PRIMARY KEY,
      phone TEXT NOT NULL UNIQUE,
      password_hash TEXT NOT NULL,
      mpin_hash TEXT NOT NULL,
      name TEXT NOT NULL,
      joined_at TEXT NOT NULL,
      referral_code TEXT NOT NULL,
      role TEXT NOT NULL DEFAULT 'user',
      approval_status TEXT NOT NULL DEFAULT 'Approved',
      approved_at TEXT,
      rejected_at TEXT,
      blocked_at TEXT,
      deactivated_at TEXT,
      status_note TEXT,
      signup_bonus_granted INTEGER NOT NULL DEFAULT 0,
      referred_by_user_id TEXT
    );

    CREATE TABLE IF NOT EXISTS sessions (
      token_hash TEXT PRIMARY KEY,
      user_id TEXT NOT NULL,
      created_at TEXT NOT NULL
    );

      CREATE TABLE IF NOT EXISTS wallet_entries (
        id TEXT PRIMARY KEY,
        user_id TEXT NOT NULL,
        type TEXT NOT NULL,
        status TEXT NOT NULL,
        amount REAL NOT NULL,
        before_balance REAL NOT NULL,
        after_balance REAL NOT NULL,
        reference_id TEXT,
        proof_url TEXT,
        note TEXT,
        created_at TEXT NOT NULL
      );

    CREATE TABLE IF NOT EXISTS bids (
      id TEXT PRIMARY KEY,
      user_id TEXT NOT NULL,
      market TEXT NOT NULL,
      board_label TEXT NOT NULL,
      session_type TEXT NOT NULL DEFAULT 'Close',
      digit TEXT NOT NULL,
      points REAL NOT NULL,
      status TEXT NOT NULL,
      payout REAL NOT NULL DEFAULT 0,
      settled_at TEXT,
      settled_result TEXT,
      created_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS bank_accounts (
      id TEXT PRIMARY KEY,
      user_id TEXT NOT NULL,
      account_number TEXT NOT NULL,
      holder_name TEXT NOT NULL,
      ifsc TEXT NOT NULL,
      created_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS notification_devices (
      id TEXT PRIMARY KEY,
      user_id TEXT NOT NULL,
      platform TEXT NOT NULL,
      token TEXT NOT NULL,
      enabled INTEGER NOT NULL DEFAULT 1,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS notifications (
      id TEXT PRIMARY KEY,
      user_id TEXT NOT NULL,
      title TEXT NOT NULL,
      body TEXT NOT NULL,
      channel TEXT NOT NULL,
      read INTEGER NOT NULL DEFAULT 0,
      created_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS payment_orders (
      id TEXT PRIMARY KEY,
      user_id TEXT NOT NULL,
      provider TEXT NOT NULL,
      amount REAL NOT NULL,
      status TEXT NOT NULL,
      reference TEXT NOT NULL UNIQUE,
      checkout_token TEXT,
      gateway_order_id TEXT,
      gateway_payment_id TEXT,
      gateway_signature TEXT,
      verified_at TEXT,
      redirect_url TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS app_settings (
      setting_key TEXT PRIMARY KEY,
      setting_value TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS markets (
      id TEXT PRIMARY KEY,
      slug TEXT NOT NULL UNIQUE,
      name TEXT NOT NULL,
      result TEXT NOT NULL,
      status TEXT NOT NULL,
      action TEXT NOT NULL,
      open_time TEXT NOT NULL,
      close_time TEXT NOT NULL,
      category TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS charts (
      market_slug TEXT NOT NULL,
      chart_type TEXT NOT NULL,
      rows_json TEXT NOT NULL,
      PRIMARY KEY (market_slug, chart_type)
    );

    CREATE TABLE IF NOT EXISTS audit_logs (
      id TEXT PRIMARY KEY,
      actor_user_id TEXT NOT NULL,
      action TEXT NOT NULL,
      entity_type TEXT NOT NULL,
      entity_id TEXT NOT NULL,
      details TEXT NOT NULL,
      created_at TEXT NOT NULL
    );
  `);

  ensureSqliteColumn(sqlite, "users", "approved_at", "TEXT");
  ensureSqliteColumn(sqlite, "users", "rejected_at", "TEXT");
  ensureSqliteColumn(sqlite, "users", "blocked_at", "TEXT");
  ensureSqliteColumn(sqlite, "users", "deactivated_at", "TEXT");
  ensureSqliteColumn(sqlite, "users", "status_note", "TEXT");
  ensureSqliteColumn(sqlite, "users", "signup_bonus_granted", "INTEGER NOT NULL DEFAULT 0");
  ensureSqliteColumn(sqlite, "users", "referred_by_user_id", "TEXT");
  ensureSqliteColumn(sqlite, "wallet_entries", "reference_id", "TEXT");
  ensureSqliteColumn(sqlite, "wallet_entries", "proof_url", "TEXT");
  ensureSqliteColumn(sqlite, "wallet_entries", "note", "TEXT");
  ensureSqliteColumn(sqlite, "payment_orders", "checkout_token", "TEXT");
  ensureSqliteColumn(sqlite, "payment_orders", "gateway_order_id", "TEXT");
  ensureSqliteColumn(sqlite, "payment_orders", "gateway_payment_id", "TEXT");
  ensureSqliteColumn(sqlite, "payment_orders", "gateway_signature", "TEXT");
  ensureSqliteColumn(sqlite, "payment_orders", "verified_at", "TEXT");

  const userCount = Number(sqlite.prepare("SELECT COUNT(*) AS count FROM users").get().count || 0);
  if (userCount === 0) {
    sqlite.prepare(`
      INSERT INTO users (id, phone, password_hash, mpin_hash, name, joined_at, referral_code, role, approval_status)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      defaultUser.id,
      defaultUser.phone,
      defaultUser.passwordHash,
      defaultUser.mpinHash,
      defaultUser.name,
      defaultUser.joinedAt,
      defaultUser.referralCode,
      defaultUser.role,
      defaultUser.approvalStatus
    );
    sqlite.prepare(`UPDATE users SET approved_at = ?, signup_bonus_granted = 1 WHERE id = ?`).run(defaultUser.joinedAt, defaultUser.id);
  }

  const walletCount = Number(sqlite.prepare("SELECT COUNT(*) AS count FROM wallet_entries").get().count || 0);
  if (walletCount === 0) {
    sqlite.prepare(`
      INSERT INTO wallet_entries (id, user_id, type, status, amount, before_balance, after_balance, created_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `).run("wallet_1", defaultUser.id, "DEPOSIT", "SUCCESS", 0, 0, 0, nowIso());
  }

  const marketCount = Number(sqlite.prepare("SELECT COUNT(*) AS count FROM markets").get().count || 0);
  if (marketCount === 0) {
    const insert = sqlite.prepare(`
      INSERT INTO markets (id, slug, name, result, status, action, open_time, close_time, category)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);
    insert.run("market_1", "mangal-bazar", "Mangal Bazar", "***-**-***", "Betting is running for close", "Place Bet", "10:05 PM", "11:05 PM", "games");
    insert.run("market_2", "bharat-starline", "Bharat Starline", "580", "Live bidding open now", "Play Now", "10:00 AM", "09:00 PM", "starline");
  }

  const chartCount = Number(sqlite.prepare("SELECT COUNT(*) AS count FROM charts").get().count || 0);
  if (chartCount === 0) {
    const sampleRows = JSON.stringify([
      ["05-Feb", "470", "237", "450"],
      ["12-Feb", "368", "125", "359"]
    ]);
    sqlite.prepare("INSERT INTO charts (market_slug, chart_type, rows_json) VALUES (?, ?, ?)").run("mangal-bazar", "jodi", sampleRows);
    sqlite.prepare("INSERT INTO charts (market_slug, chart_type, rows_json) VALUES (?, ?, ?)").run("mangal-bazar", "panna", sampleRows);
  }

  const settingsCount = Number(sqlite.prepare("SELECT COUNT(*) AS count FROM app_settings").get().count || 0);
  if (settingsCount === 0) {
    const insertSetting = sqlite.prepare(`INSERT INTO app_settings (setting_key, setting_value, updated_at) VALUES (?, ?, ?)`);
    const createdAt = nowIso();
    insertSetting.run("notice_text", "Withdraw approvals aur result updates yahan se control hote hain.", createdAt);
    insertSetting.run("support_phone", defaultUser.phone, createdAt);
    insertSetting.run("support_hours", "10:00 AM - 10:00 PM", createdAt);
    insertSetting.run("bonus_enabled", "true", createdAt);
    insertSetting.run("bonus_text", "Signup bonus aur promo offers ko dashboard se monitor karo.", createdAt);
  }

  return sqlite;
}

function getPgPool() {
  if (!isStandalonePostgresEnabled()) {
    return null;
  }

  if (!pgPool) {
    const normalizedUrl = new URL(standaloneConfig.databaseUrl);
    normalizedUrl.searchParams.delete("sslmode");
    pgPool = new Pool({
      connectionString: normalizedUrl.toString(),
      ssl: isLocalPostgresUrl(standaloneConfig.databaseUrl) ? false : { rejectUnauthorized: false }
    });
  }

  void ensurePostgresBootstrap(pgPool);
  return pgPool;
}

async function getReadyPgPool() {
  const pool = getPgPool();
  await ensurePostgresBootstrap(pool);
  return pool;
}

export async function findUserByPhone(phone) {
  if (isStandalonePostgresEnabled()) {
    const pool = await getReadyPgPool();
    const result = await pool.query(
      `SELECT id, phone, password_hash, mpin_hash, name, role, referral_code, joined_at, approval_status, approved_at, rejected_at, blocked_at, deactivated_at, status_note, signup_bonus_granted, referred_by_user_id
       FROM users
       WHERE phone = $1
       LIMIT 1`,
      [phone]
    );
    return mapUserRow(result.rows[0]);
  }

  const row = getSqlite()
    .prepare(
      `SELECT id, phone, password_hash, mpin_hash, name, role, referral_code, joined_at, approval_status, approved_at, rejected_at, blocked_at, deactivated_at, status_note, signup_bonus_granted, referred_by_user_id
       FROM users
       WHERE phone = ?
       LIMIT 1`
    )
    .get(phone);
  return mapUserRow(row);
}

export async function createSession(userId) {
  const rawToken = randomBytes(24).toString("hex");
  const tokenHash = hashSecret(rawToken);
  const createdAt = nowIso();

  if (isStandalonePostgresEnabled()) {
      const pool = await getReadyPgPool();
    await pool.query(
      `INSERT INTO sessions (token_hash, user_id, created_at)
       VALUES ($1, $2, $3)`,
      [tokenHash, userId, createdAt]
    );
  } else {
    getSqlite()
      .prepare(`INSERT INTO sessions (token_hash, user_id, created_at) VALUES (?, ?, ?)`)
      .run(tokenHash, userId, createdAt);
  }

  return { rawToken, tokenHash, createdAt };
}

export async function revokeSession(token) {
  if (!token) {
    return;
  }

  const tokenHash = hashSecret(token);
  if (isStandalonePostgresEnabled()) {
      const pool = await getReadyPgPool();
    await pool.query("DELETE FROM sessions WHERE token_hash = $1", [tokenHash]);
    return;
  }

  getSqlite().prepare("DELETE FROM sessions WHERE token_hash = ?").run(tokenHash);
}

export async function requireUserByToken(token) {
  if (!token) {
    return null;
  }

  const tokenHash = hashSecret(token);
  const minCreatedAt = new Date(Date.now() - sessionTtlMs).toISOString();

  if (isStandalonePostgresEnabled()) {
    const pool = await getReadyPgPool();
    const result = await pool.query(
      `SELECT u.id, u.phone, u.password_hash, u.mpin_hash, u.name, u.role, u.referral_code, u.joined_at, u.approval_status, u.approved_at, u.rejected_at, u.blocked_at, u.deactivated_at, u.status_note, u.signup_bonus_granted, u.referred_by_user_id
       FROM sessions s
       JOIN users u ON u.id = s.user_id
       WHERE s.token_hash = $1 AND s.created_at >= $2
       LIMIT 1`,
      [tokenHash, minCreatedAt]
    );
    return mapUserRow(result.rows[0]);
  }

  const row = getSqlite()
    .prepare(
      `SELECT u.id, u.phone, u.password_hash, u.mpin_hash, u.name, u.role, u.referral_code, u.joined_at, u.approval_status, u.approved_at, u.rejected_at, u.blocked_at, u.deactivated_at, u.status_note, u.signup_bonus_granted, u.referred_by_user_id
       FROM sessions s
       JOIN users u ON u.id = s.user_id
       WHERE s.token_hash = ? AND s.created_at >= ?
       LIMIT 1`
    )
    .get(tokenHash, minCreatedAt);
  return mapUserRow(row);
}

export async function getUserBalance(userId) {
  if (isStandalonePostgresEnabled()) {
    const pool = await getReadyPgPool();
    const result = await pool.query(
      `SELECT after_balance
       FROM wallet_entries
       WHERE user_id = $1
       ORDER BY created_at DESC
       LIMIT 1`,
      [userId]
    );
    return Number(result.rows[0]?.after_balance ?? 0);
  }

  const row = getSqlite()
    .prepare(
      `SELECT after_balance
       FROM wallet_entries
       WHERE user_id = ?
       ORDER BY created_at DESC
       LIMIT 1`
    )
    .get(userId);
  return Number(row?.after_balance ?? 0);
}

export async function updateUserPassword(userId, passwordHash) {
  if (isStandalonePostgresEnabled()) {
      const pool = await getReadyPgPool();
    await pool.query("UPDATE users SET password_hash = $2 WHERE id = $1", [userId, passwordHash]);
    return;
  }

  getSqlite().prepare("UPDATE users SET password_hash = ? WHERE id = ?").run(passwordHash, userId);
}

export async function updateUserMpin(userId, mpinHash) {
  if (isStandalonePostgresEnabled()) {
      const pool = await getReadyPgPool();
    await pool.query("UPDATE users SET mpin_hash = $2 WHERE id = $1", [userId, mpinHash]);
    return;
  }

  getSqlite().prepare("UPDATE users SET mpin_hash = ? WHERE id = ?").run(mpinHash, userId);
}

export async function updateUserProfile(userId, updates) {
  const nextName = typeof updates.name === "string" ? updates.name.trim() : "";
  const nextPhone = typeof updates.phone === "string" ? updates.phone.trim() : "";

  if (isStandalonePostgresEnabled()) {
      const pool = await getReadyPgPool();
    const result = await pool.query(
      `UPDATE users
       SET name = COALESCE(NULLIF($2, ''), name),
           phone = COALESCE(NULLIF($3, ''), phone)
       WHERE id = $1
       RETURNING id, phone, password_hash, mpin_hash, name, role, referral_code, joined_at, approval_status, approved_at, rejected_at, blocked_at, deactivated_at, status_note, signup_bonus_granted, referred_by_user_id`,
      [userId, nextName, nextPhone]
    );
    return mapUserRow(result.rows[0]);
  }

  const db = getSqlite();
  db.prepare(
    `UPDATE users
     SET name = COALESCE(NULLIF(?, ''), name),
         phone = COALESCE(NULLIF(?, ''), phone)
     WHERE id = ?`
  ).run(nextName, nextPhone, userId);

  const row = db
    .prepare(
      `SELECT id, phone, password_hash, mpin_hash, name, role, referral_code, joined_at, approval_status, approved_at, rejected_at, blocked_at, deactivated_at, status_note, signup_bonus_granted, referred_by_user_id
       FROM users
       WHERE id = ?
       LIMIT 1`
    )
    .get(userId);
  return mapUserRow(row);
}

export async function createUserAccount({ phone, passwordHash, referenceCode }) {
  const existing = await findUserByPhone(phone);
  if (existing) {
    return { user: null, error: "Phone number already registered" };
  }

  const normalizedReferenceCode = String(referenceCode ?? "").trim();
  const referrer = normalizedReferenceCode ? await findUserByReferralCode(normalizedReferenceCode) : null;
  if (normalizedReferenceCode && !referrer) {
    return { user: null, error: "Invalid reference code" };
  }

  const userId = `user_${Date.now()}`;
  const joinedAt = nowIso();
  const referralCode = String(Math.floor(100000 + Math.random() * 900000));
  const name = `User ${phone.slice(-4)}`;

  if (isStandalonePostgresEnabled()) {
      const pool = await getReadyPgPool();
    await pool.query(
      `INSERT INTO users (id, phone, password_hash, mpin_hash, name, joined_at, referral_code, role, approval_status, approved_at, signup_bonus_granted, referred_by_user_id)
       VALUES ($1, $2, $3, $4, $5, $6, $7, 'user', 'Approved', $6, FALSE, $8)`,
      [userId, phone, passwordHash, hashSecret("1234"), name, joinedAt, referralCode, referrer?.id ?? null]
    );
  } else {
    getSqlite()
      .prepare(
        `INSERT INTO users (id, phone, password_hash, mpin_hash, name, joined_at, referral_code, role, approval_status, approved_at, signup_bonus_granted, referred_by_user_id)
         VALUES (?, ?, ?, ?, ?, ?, ?, 'user', 'Approved', ?, 0, ?)`
      )
      .run(userId, phone, passwordHash, hashSecret("1234"), name, joinedAt, referralCode, joinedAt, referrer?.id ?? null);
  }

  return {
    user: {
      id: userId,
      phone,
      name,
      role: "user",
      referralCode,
      joinedAt,
      approvalStatus: "Approved",
      approvedAt: joinedAt,
      rejectedAt: null,
      signupBonusGranted: false,
      referredByUserId: referrer.id
    },
    error: null
  };
}

async function findUserByReferralCode(referenceCode) {
  if (isStandalonePostgresEnabled()) {
      const pool = await getReadyPgPool();
    const result = await pool.query(
      `SELECT id, phone, password_hash, name, role, referral_code, joined_at, approval_status, approved_at, rejected_at, blocked_at, deactivated_at, status_note, signup_bonus_granted, referred_by_user_id
       FROM users
       WHERE referral_code = $1
       LIMIT 1`,
      [referenceCode]
    );
    return mapUserRow(result.rows[0]);
  }

  return mapUserRow(
    getSqlite()
      .prepare(
        `SELECT id, phone, password_hash, mpin_hash, name, role, referral_code, joined_at, approval_status, approved_at, rejected_at, blocked_at, deactivated_at, status_note, signup_bonus_granted, referred_by_user_id
         FROM users
         WHERE referral_code = ?
         LIMIT 1`
      )
      .get(referenceCode)
  );
}

export async function findUserById(userId) {
  if (isStandalonePostgresEnabled()) {
      const pool = await getReadyPgPool();
    const result = await pool.query(
      `SELECT id, phone, password_hash, mpin_hash, name, joined_at, referral_code, role, approval_status, approved_at, rejected_at, blocked_at, deactivated_at, status_note, signup_bonus_granted, referred_by_user_id
       FROM users
       WHERE id = $1
       LIMIT 1`,
      [userId]
    );
    return mapUserRow(result.rows[0]);
  }

  return mapUserRow(
    getSqlite()
      .prepare(
        `SELECT id, phone, password_hash, mpin_hash, name, joined_at, referral_code, role, approval_status, approved_at, rejected_at, blocked_at, deactivated_at, status_note, signup_bonus_granted, referred_by_user_id
         FROM users
         WHERE id = ?
         LIMIT 1`
      )
      .get(userId)
  );
}

export async function getUsersList() {
  if (isStandalonePostgresEnabled()) {
      const pool = await getReadyPgPool();
    const result = await pool.query(
      `SELECT id, phone, password_hash, mpin_hash, name, joined_at, referral_code, role, approval_status, approved_at, rejected_at, blocked_at, deactivated_at, status_note, signup_bonus_granted, referred_by_user_id
       FROM users
       ORDER BY joined_at DESC, id DESC`
    );
    return result.rows.map((row) => mapUserRow(row));
  }

  return getSqlite()
    .prepare(
      `SELECT id, phone, password_hash, mpin_hash, name, joined_at, referral_code, role, approval_status, approved_at, rejected_at, blocked_at, deactivated_at, status_note, signup_bonus_granted, referred_by_user_id
       FROM users
       ORDER BY joined_at DESC, id DESC`
    )
    .all()
    .map((row) => mapUserRow(row));
}

export async function getWalletEntriesForUser(userId) {
  if (isStandalonePostgresEnabled()) {
    const pool = await getReadyPgPool();
    const result = await pool.query(
      `SELECT id, user_id, type, status, amount, before_balance, after_balance, reference_id, proof_url, note, created_at
         FROM wallet_entries
         WHERE user_id = $1
         ORDER BY created_at DESC
         LIMIT 50`,
      [userId]
    );
    return result.rows.map((row) => mapWalletEntryRow(row));
  }

  const rows = getSqlite()
    .prepare(
      `SELECT id, user_id, type, status, amount, before_balance, after_balance, reference_id, proof_url, note, created_at
         FROM wallet_entries
         WHERE user_id = ?
         ORDER BY created_at DESC
         LIMIT 50`
    )
    .all(userId);

  return rows.map((row) => mapWalletEntryRow(row));
}

export async function getBidsForUser(userId) {
  if (isStandalonePostgresEnabled()) {
    const pool = await getReadyPgPool();
    const result = await pool.query(
      `SELECT id, market, board_label, session_type, digit, points, status, payout, settled_at, settled_result, created_at
       FROM bids
       WHERE user_id = $1
       ORDER BY created_at DESC
       LIMIT 50`,
      [userId]
    );
    return result.rows.map((row) => ({
      id: row.id,
      market: row.market,
      boardLabel: row.board_label,
      sessionType: row.session_type,
      digit: row.digit,
      points: Number(row.points),
      status: row.status,
      payout: Number(row.payout ?? 0),
      settledAt: row.settled_at ? (row.settled_at instanceof Date ? row.settled_at.toISOString() : String(row.settled_at)) : null,
      settledResult: row.settled_result ?? null,
      createdAt: row.created_at instanceof Date ? row.created_at.toISOString() : String(row.created_at)
    }));
  }

  const rows = getSqlite()
    .prepare(
      `SELECT id, market, board_label, session_type, digit, points, status, payout, settled_at, settled_result, created_at
       FROM bids
       WHERE user_id = ?
       ORDER BY created_at DESC
       LIMIT 50`
    )
    .all(userId);

  return rows.map((row) => ({
    id: row.id,
    market: row.market,
    boardLabel: row.board_label,
    sessionType: row.session_type,
    digit: row.digit,
    points: Number(row.points),
    status: row.status,
    payout: Number(row.payout ?? 0),
    settledAt: row.settled_at ?? null,
    settledResult: row.settled_result ?? null,
    createdAt: row.created_at
  }));
}

export async function getBankAccountsForUser(userId) {
  if (isStandalonePostgresEnabled()) {
    const pool = await getReadyPgPool();
    const result = await pool.query(
      `SELECT id, account_number, holder_name, ifsc, created_at
       FROM bank_accounts
       WHERE user_id = $1
       ORDER BY created_at DESC`,
      [userId]
    );
    return result.rows.map((row) => ({
      id: row.id,
      accountNumber: row.account_number,
      holderName: row.holder_name,
      ifsc: row.ifsc,
      createdAt: row.created_at instanceof Date ? row.created_at.toISOString() : String(row.created_at)
    }));
  }

  const rows = getSqlite()
    .prepare(
      `SELECT id, account_number, holder_name, ifsc, created_at
       FROM bank_accounts
       WHERE user_id = ?
       ORDER BY created_at DESC`
    )
    .all(userId);

  return rows.map((row) => ({
    id: row.id,
    accountNumber: row.account_number,
    holderName: row.holder_name,
    ifsc: row.ifsc,
    createdAt: row.created_at
  }));
}

export async function addBankAccount({ userId, accountNumber, holderName, ifsc }) {
  const id = `bank_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
  const createdAt = nowIso();

  if (isStandalonePostgresEnabled()) {
      const pool = await getReadyPgPool();
    await pool.query(
      `INSERT INTO bank_accounts (id, user_id, account_number, holder_name, ifsc, created_at)
       VALUES ($1, $2, $3, $4, $5, $6)`,
      [id, userId, accountNumber, holderName, ifsc, createdAt]
    );
  } else {
    getSqlite()
      .prepare(
        `INSERT INTO bank_accounts (id, user_id, account_number, holder_name, ifsc, created_at)
         VALUES (?, ?, ?, ?, ?, ?)`
      )
      .run(id, userId, accountNumber, holderName, ifsc, createdAt);
  }

  return { id, accountNumber, holderName, ifsc, createdAt };
}

export async function addWalletEntry({ userId, type, status, amount, beforeBalance, afterBalance, referenceId = "", proofUrl = "", note = "" }) {
  const id = `wallet_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
  const createdAt = nowIso();

  if (isStandalonePostgresEnabled()) {
    const pool = getPgPool();
    await pool.query(
      `INSERT INTO wallet_entries (id, user_id, type, status, amount, before_balance, after_balance, reference_id, proof_url, note, created_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
      [id, userId, type, status, amount, beforeBalance, afterBalance, referenceId || null, proofUrl || null, note || null, createdAt]
    );
  } else {
    getSqlite()
      .prepare(
        `INSERT INTO wallet_entries (id, user_id, type, status, amount, before_balance, after_balance, reference_id, proof_url, note, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
      )
      .run(id, userId, type, status, amount, beforeBalance, afterBalance, referenceId || null, proofUrl || null, note || null, createdAt);
  }

  return { id, userId, type, status, amount, beforeBalance, afterBalance, referenceId, proofUrl, note, createdAt };
}

export async function addBid({ userId, market, boardLabel, sessionType, digit, points, status, payout, settledAt, settledResult }) {
  const id = `bid_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
  const createdAt = nowIso();

  if (isStandalonePostgresEnabled()) {
    const pool = getPgPool();
    await pool.query(
      `INSERT INTO bids (id, user_id, market, board_label, session_type, digit, points, status, payout, settled_at, settled_result, created_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
      [id, userId, market, boardLabel, sessionType, digit, points, status, payout, settledAt, settledResult, createdAt]
    );
  } else {
    getSqlite()
      .prepare(
        `INSERT INTO bids (id, user_id, market, board_label, session_type, digit, points, status, payout, settled_at, settled_result, created_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
      )
      .run(id, userId, market, boardLabel, sessionType, digit, points, status, payout, settledAt, settledResult, createdAt);
  }

  return { id, userId, market, boardLabel, sessionType, digit, points, status, payout, settledAt, settledResult, createdAt };
}

export async function listMarkets() {
  if (isStandalonePostgresEnabled()) {
    const pool = await getReadyPgPool();
    const result = await pool.query(`SELECT id, slug, name, result, status, action, open_time, close_time, category FROM markets ORDER BY id ASC`);
    return sortMarketsByOpenTime(result.rows.map((row) => ({
      id: row.id,
      slug: row.slug,
      name: row.name,
      result: row.result,
      status: row.status,
      action: row.action,
      open: row.open_time,
      close: row.close_time,
      category: row.category
    })));
  }

  const rows = getSqlite().prepare(`SELECT id, slug, name, result, status, action, open_time, close_time, category FROM markets ORDER BY id ASC`).all();
  return sortMarketsByOpenTime(rows.map((row) => ({
    id: row.id,
    slug: row.slug,
    name: row.name,
    result: row.result,
    status: row.status,
    action: row.action,
    open: row.open_time,
    close: row.close_time,
    category: row.category
  })));
}

export async function findMarketBySlug(slug) {
  const markets = await listMarkets();
  return markets.find((item) => item.slug === slug) ?? null;
}

export async function getChartRecord(slug, chartType) {
  if (isStandalonePostgresEnabled()) {
    const pool = await getReadyPgPool();
    const result = await pool.query(
      `SELECT market_slug, chart_type, rows_json
       FROM charts
       WHERE market_slug = $1 AND chart_type = $2
       LIMIT 1`,
      [slug, chartType]
    );
    const row = result.rows[0];
    return row
      ? { marketSlug: row.market_slug, chartType: row.chart_type, rows: normalizeChartRowsForStorage(chartType, toChartRows(row.rows_json)) }
      : null;
  }

  const row = getSqlite()
    .prepare(
      `SELECT market_slug, chart_type, rows_json
       FROM charts
       WHERE market_slug = ? AND chart_type = ?
       LIMIT 1`
    )
    .get(slug, chartType);
  return row
    ? { marketSlug: row.market_slug, chartType: row.chart_type, rows: normalizeChartRowsForStorage(chartType, toChartRows(row.rows_json)) }
    : null;
}

export async function upsertChartRecord(marketSlug, chartType, rows) {
  const normalizedRows = normalizeChartRowsForStorage(chartType, rows);
  if (isStandalonePostgresEnabled()) {
    const pool = await getReadyPgPool();
    const result = await pool.query(
      `INSERT INTO charts (market_slug, chart_type, rows_json)
       VALUES ($1, $2, $3)
       ON CONFLICT (market_slug, chart_type) DO UPDATE SET rows_json = EXCLUDED.rows_json
       RETURNING market_slug, chart_type, rows_json`,
      [marketSlug, chartType, JSON.stringify(normalizedRows)]
    );
    const row = result.rows[0];
    return row
      ? { marketSlug: row.market_slug, chartType: row.chart_type, rows: normalizeChartRowsForStorage(chartType, toChartRows(row.rows_json)) }
      : null;
  }

  const db = getSqlite();
  db.prepare(
    `INSERT INTO charts (market_slug, chart_type, rows_json)
     VALUES (?, ?, ?)
     ON CONFLICT(market_slug, chart_type) DO UPDATE SET rows_json = excluded.rows_json`
  ).run(marketSlug, chartType, JSON.stringify(normalizedRows));

  return getChartRecord(marketSlug, chartType);
}

export async function updateMarketRecord(slug, updates) {
  const current = await findMarketBySlug(slug);
  if (!current) {
    return null;
  }

  const next = {
    result: updates.result?.trim() || current.result,
    status: updates.status?.trim() || current.status,
    action: updates.action?.trim() || current.action,
    open: updates.open?.trim() || current.open,
    close: updates.close?.trim() || current.close,
    category: updates.category || current.category
  };

  if (isStandalonePostgresEnabled()) {
    const pool = getPgPool();
    await pool.query(
      `UPDATE markets
       SET result = $1, status = $2, action = $3, open_time = $4, close_time = $5, category = $6
       WHERE slug = $7`,
      [next.result, next.status, next.action, next.open, next.close, next.category, slug]
    );
  } else {
    getSqlite()
      .prepare(
        `UPDATE markets
         SET result = ?, status = ?, action = ?, open_time = ?, close_time = ?, category = ?
         WHERE slug = ?`
      )
      .run(next.result, next.status, next.action, next.open, next.close, next.category, slug);
  }

  return findMarketBySlug(slug);
}

export async function getBidsForMarket(marketName) {
  if (isStandalonePostgresEnabled()) {
    const pool = await getReadyPgPool();
    const result = await pool.query(
      `SELECT id, user_id, market, board_label, session_type, digit, points, status, payout, settled_at, settled_result, created_at
       FROM bids
       WHERE market = $1
       ORDER BY created_at ASC, id ASC`,
      [marketName]
    );
    return result.rows.map((row) => mapBidRow(row));
  }

  return getSqlite()
    .prepare(
      `SELECT id, user_id, market, board_label, session_type, digit, points, status, payout, settled_at, settled_result, created_at
       FROM bids
       WHERE market = ?
       ORDER BY created_at ASC, id ASC`
    )
    .all(marketName)
    .map((row) => mapBidRow(row));
}

export async function updateBidSettlement(bidId, status, payout, settledResult) {
  const settledAt = status === "Pending" ? null : nowIso();
  const normalizedResult = status === "Pending" ? null : settledResult;

  if (isStandalonePostgresEnabled()) {
    const pool = getPgPool();
    const result = await pool.query(
      `UPDATE bids
       SET status = $1, payout = $2, settled_at = $3, settled_result = $4
       WHERE id = $5
       RETURNING id, user_id, market, board_label, session_type, digit, points, status, payout, settled_at, settled_result, created_at`,
      [status, payout, settledAt, normalizedResult, bidId]
    );
    return mapBidRow(result.rows[0]);
  }

  const db = getSqlite();
  db.prepare(`UPDATE bids SET status = ?, payout = ?, settled_at = ?, settled_result = ? WHERE id = ?`).run(
    status,
    payout,
    settledAt,
    normalizedResult,
    bidId
  );
  return mapBidRow(
    db.prepare(
      `SELECT id, user_id, market, board_label, session_type, digit, points, status, payout, settled_at, settled_result, created_at
       FROM bids WHERE id = ? LIMIT 1`
    ).get(bidId)
  );
}

export async function listNotificationsForUser(userId) {
  if (isStandalonePostgresEnabled()) {
    const pool = getPgPool();
    const result = await pool.query(
      `SELECT id, title, body, channel, read, created_at
       FROM notifications
       WHERE user_id = $1
       ORDER BY created_at DESC`,
      [userId]
    );
    return result.rows.map((row) => ({
      id: row.id,
      title: row.title,
      body: row.body,
      channel: row.channel,
      read: Boolean(row.read),
      createdAt: row.created_at instanceof Date ? row.created_at.toISOString() : String(row.created_at)
    }));
  }

  const rows = getSqlite()
    .prepare(
      `SELECT id, title, body, channel, read, created_at
       FROM notifications
       WHERE user_id = ?
       ORDER BY created_at DESC`
    )
    .all(userId);
  return rows.map((row) => ({
    id: row.id,
    title: row.title,
    body: row.body,
    channel: row.channel,
    read: Boolean(row.read),
    createdAt: row.created_at
  }));
}

export async function registerNotificationDevice(userId, platform, token) {
  const id = `device_${Date.now()}`;
  const createdAt = nowIso();

  if (isStandalonePostgresEnabled()) {
    const pool = getPgPool();
    await pool.query(
      `INSERT INTO notification_devices (id, user_id, platform, token, enabled, created_at, updated_at)
       VALUES ($1, $2, $3, $4, TRUE, $5, $5)`,
      [id, userId, platform, token, createdAt]
    );
  } else {
    getSqlite()
      .prepare(
        `INSERT INTO notification_devices (id, user_id, platform, token, enabled, created_at, updated_at)
         VALUES (?, ?, ?, ?, 1, ?, ?)`
      )
      .run(id, userId, platform, token, createdAt, createdAt);
  }

  return { id, userId, platform, token, enabled: true, createdAt, updatedAt: createdAt };
}

export async function createNotification({ userId, title, body, channel = "general" }) {
  const id = `notification_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
  const createdAt = nowIso();

  if (isStandalonePostgresEnabled()) {
    const pool = await getReadyPgPool();
    await pool.query(
      `INSERT INTO notifications (id, user_id, title, body, channel, read, created_at)
       VALUES ($1, $2, $3, $4, $5, FALSE, $6)`,
      [id, userId, title, body, channel, createdAt]
    );
  } else {
    getSqlite()
      .prepare(
        `INSERT INTO notifications (id, user_id, title, body, channel, read, created_at)
         VALUES (?, ?, ?, ?, ?, 0, ?)`
      )
      .run(id, userId, title, body, channel, createdAt);
  }

  return { id, userId, title, body, channel, read: false, createdAt };
}

export async function listAllNotifications(limit = 200) {
  if (isStandalonePostgresEnabled()) {
    const pool = await getReadyPgPool();
    const result = await pool.query(
      `SELECT id, user_id, title, body, channel, read, created_at
       FROM notifications
       ORDER BY created_at DESC
       LIMIT $1`,
      [limit]
    );
    return result.rows.map((row) => ({
      id: row.id,
      userId: row.user_id,
      title: row.title,
      body: row.body,
      channel: row.channel,
      read: Boolean(row.read),
      createdAt: toIso(row.created_at)
    }));
  }

  return getSqlite()
    .prepare(
      `SELECT id, user_id, title, body, channel, read, created_at
       FROM notifications
       ORDER BY created_at DESC
       LIMIT ?`
    )
    .all(limit)
    .map((row) => ({
      id: row.id,
      userId: row.user_id,
      title: row.title,
      body: row.body,
      channel: row.channel,
      read: Boolean(row.read),
      createdAt: toIso(row.created_at)
    }));
}

export async function getAppSettings() {
  if (isStandalonePostgresEnabled()) {
    const pool = await getReadyPgPool();
    const result = await pool.query(`SELECT setting_key, setting_value, updated_at FROM app_settings ORDER BY setting_key ASC`);
    return result.rows.map((row) => mapAppSettingRow(row));
  }

  return getSqlite()
    .prepare(`SELECT setting_key, setting_value, updated_at FROM app_settings ORDER BY setting_key ASC`)
    .all()
    .map((row) => mapAppSettingRow(row));
}

export async function upsertAppSetting(settingKey, settingValue) {
  const updatedAt = nowIso();

  if (isStandalonePostgresEnabled()) {
    const pool = await getReadyPgPool();
    const result = await pool.query(
      `INSERT INTO app_settings (setting_key, setting_value, updated_at)
       VALUES ($1, $2, $3)
       ON CONFLICT (setting_key) DO UPDATE SET setting_value = EXCLUDED.setting_value, updated_at = EXCLUDED.updated_at
       RETURNING setting_key, setting_value, updated_at`,
      [settingKey, settingValue, updatedAt]
    );
    return mapAppSettingRow(result.rows[0]);
  }

  getSqlite()
    .prepare(
      `INSERT INTO app_settings (setting_key, setting_value, updated_at)
       VALUES (?, ?, ?)
       ON CONFLICT(setting_key) DO UPDATE SET setting_value = excluded.setting_value, updated_at = excluded.updated_at`
    )
    .run(settingKey, settingValue, updatedAt);

  return mapAppSettingRow(
    getSqlite().prepare(`SELECT setting_key, setting_value, updated_at FROM app_settings WHERE setting_key = ? LIMIT 1`).get(settingKey)
  );
}

export async function updateUserAccountStatus(userId, action, note = "") {
  const user = await findUserById(userId);
  if (!user) {
    return null;
  }

  const blockedAt = action === "block" ? nowIso() : action === "unblock" ? null : user.blockedAt;
  const deactivatedAt = action === "deactivate" ? nowIso() : action === "activate" ? null : user.deactivatedAt;
  const statusNote = note.trim();

  if (isStandalonePostgresEnabled()) {
    const pool = await getReadyPgPool();
    const result = await pool.query(
      `UPDATE users
       SET blocked_at = $2,
           deactivated_at = $3,
           status_note = $4
       WHERE id = $1
       RETURNING id, phone, password_hash, mpin_hash, name, joined_at, referral_code, role, approval_status, approved_at, rejected_at, blocked_at, deactivated_at, status_note, signup_bonus_granted, referred_by_user_id`,
      [userId, blockedAt, deactivatedAt, statusNote]
    );
    return mapUserRow(result.rows[0]);
  }

  getSqlite()
    .prepare(
      `UPDATE users
       SET blocked_at = ?, deactivated_at = ?, status_note = ?
       WHERE id = ?`
    )
    .run(blockedAt, deactivatedAt, statusNote, userId);

  return findUserById(userId);
}

export async function listAllBids(limit = 300) {
  if (isStandalonePostgresEnabled()) {
    const pool = await getReadyPgPool();
    const result = await pool.query(
      `SELECT id, user_id, market, board_label, session_type, digit, points, status, payout, settled_at, settled_result, created_at
       FROM bids
       ORDER BY created_at DESC, id DESC
       LIMIT $1`,
      [limit]
    );
    return result.rows.map((row) => mapBidRow(row));
  }

  return getSqlite()
    .prepare(
      `SELECT id, user_id, market, board_label, session_type, digit, points, status, payout, settled_at, settled_result, created_at
       FROM bids
       ORDER BY created_at DESC, id DESC
       LIMIT ?`
    )
    .all(limit)
    .map((row) => mapBidRow(row));
}

async function findPaymentOrderById(paymentOrderId) {
  if (isStandalonePostgresEnabled()) {
    const pool = getPgPool();
    const result = await pool.query(
      `SELECT id, user_id, provider, amount, status, reference, checkout_token, gateway_order_id, gateway_payment_id, gateway_signature, verified_at, redirect_url, created_at, updated_at
       FROM payment_orders
       WHERE id = $1
       LIMIT 1`,
      [paymentOrderId]
    );
    return mapPaymentOrderRow(result.rows[0]);
  }

  return mapPaymentOrderRow(
    getSqlite()
      .prepare(
        `SELECT id, user_id, provider, amount, status, reference, checkout_token, gateway_order_id, gateway_payment_id, gateway_signature, verified_at, redirect_url, created_at, updated_at
         FROM payment_orders
         WHERE id = ?
         LIMIT 1`
      )
      .get(paymentOrderId)
  );
}

async function findPaymentOrderByReference(reference) {
  if (!reference) {
    return null;
  }

  if (isStandalonePostgresEnabled()) {
    const pool = getPgPool();
    const result = await pool.query(
      `SELECT id, user_id, provider, amount, status, reference, checkout_token, gateway_order_id, gateway_payment_id, gateway_signature, verified_at, redirect_url, created_at, updated_at
       FROM payment_orders
       WHERE reference = $1
       LIMIT 1`,
      [reference]
    );
    return mapPaymentOrderRow(result.rows[0]);
  }

  return mapPaymentOrderRow(
    getSqlite()
      .prepare(
        `SELECT id, user_id, provider, amount, status, reference, checkout_token, gateway_order_id, gateway_payment_id, gateway_signature, verified_at, redirect_url, created_at, updated_at
         FROM payment_orders
         WHERE reference = ?
         LIMIT 1`
      )
      .get(reference)
  );
}

export async function findPaymentOrderForCheckout(paymentOrderId, checkoutToken) {
  const order = await findPaymentOrderById(paymentOrderId);
  if (!order || !checkoutToken || order.checkoutToken !== checkoutToken) {
    return null;
  }
  return order;
}

export async function createPaymentOrder({
  id = `payment_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
  userId,
  amount,
  provider = "manual",
  reference = `RM${Date.now()}`,
  checkoutToken = null,
  gatewayOrderId = null,
  redirectUrl = null
}) {
  const createdAt = nowIso();
  const status = "PENDING";

  if (isStandalonePostgresEnabled()) {
    const pool = getPgPool();
    await pool.query(
      `INSERT INTO payment_orders (id, user_id, provider, amount, status, reference, checkout_token, gateway_order_id, redirect_url, created_at, updated_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $10)`,
      [id, userId, provider, amount, status, reference, checkoutToken, gatewayOrderId, redirectUrl, createdAt]
    );
  } else {
    getSqlite()
      .prepare(
        `INSERT INTO payment_orders (id, user_id, provider, amount, status, reference, checkout_token, gateway_order_id, redirect_url, created_at, updated_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
      )
      .run(id, userId, provider, amount, status, reference, checkoutToken, gatewayOrderId, redirectUrl, createdAt, createdAt);
  }

  return findPaymentOrderById(id);
}

export async function completePaymentOrder({ paymentOrderId, gatewayOrderId, gatewayPaymentId, gatewaySignature }) {
  const verifiedAt = nowIso();

  if (isStandalonePostgresEnabled()) {
    const pool = getPgPool();
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      const existingResult = await client.query(
        `SELECT id, user_id, provider, amount, status, reference, checkout_token, gateway_order_id, gateway_payment_id, gateway_signature, verified_at, redirect_url, created_at, updated_at
         FROM payment_orders
         WHERE id = $1
         FOR UPDATE`,
        [paymentOrderId]
      );
      const existing = existingResult.rows[0];
      if (!existing) {
        await client.query("ROLLBACK");
        return null;
      }
      if (existing.gateway_order_id && existing.gateway_order_id !== gatewayOrderId) {
        throw new Error("Gateway order mismatch");
      }
      if (existing.status !== "SUCCESS") {
        const currentBalance = Number(
          (
            await client.query(
              `SELECT COALESCE(
                 (
                   SELECT after_balance
                   FROM wallet_entries
                   WHERE user_id = $1
                   ORDER BY created_at DESC, id DESC
                   LIMIT 1
                 ),
                 0
               ) AS balance`,
              [existing.user_id]
            )
          ).rows[0]?.balance ?? 0
        );
        const nextBalance = currentBalance + Number(existing.amount);
        await client.query(
          `UPDATE payment_orders
           SET status = 'SUCCESS',
               gateway_order_id = $2,
               gateway_payment_id = $3,
               gateway_signature = $4,
               verified_at = $5,
               updated_at = $5
           WHERE id = $1`,
          [paymentOrderId, gatewayOrderId, gatewayPaymentId, gatewaySignature, verifiedAt]
        );
        await client.query(
          `INSERT INTO wallet_entries (id, user_id, type, status, amount, before_balance, after_balance, reference_id, note, created_at)
           VALUES ($1, $2, 'DEPOSIT', 'SUCCESS', $3, $4, $5, $6, $7, $8)`,
          [
            `wallet_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
            existing.user_id,
            Number(existing.amount),
            currentBalance,
            nextBalance,
            gatewayPaymentId,
            `Razorpay payment ${gatewayPaymentId}`,
            verifiedAt
          ]
        );
      }
      await client.query("COMMIT");
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }

    return findPaymentOrderById(paymentOrderId);
  }

  const db = getSqlite();
  db.exec("BEGIN");
  try {
    const existing = db
      .prepare(
        `SELECT id, user_id, provider, amount, status, reference, checkout_token, gateway_order_id, gateway_payment_id, gateway_signature, verified_at, redirect_url, created_at, updated_at
         FROM payment_orders
         WHERE id = ?
         LIMIT 1`
      )
      .get(paymentOrderId);
    if (!existing) {
      db.exec("ROLLBACK");
      return null;
    }
    if (existing.gateway_order_id && existing.gateway_order_id !== gatewayOrderId) {
      throw new Error("Gateway order mismatch");
    }
    if (existing.status !== "SUCCESS") {
      const currentBalance = Number(
        db
          .prepare(
            `SELECT COALESCE(
               (
                 SELECT after_balance
                 FROM wallet_entries
                 WHERE user_id = ?
                 ORDER BY created_at DESC, id DESC
                 LIMIT 1
               ),
               0
             ) AS balance`
          )
          .get(existing.user_id)?.balance ?? 0
      );
      const nextBalance = currentBalance + Number(existing.amount);
      db.prepare(
        `UPDATE payment_orders
         SET status = 'SUCCESS',
             gateway_order_id = ?,
             gateway_payment_id = ?,
             gateway_signature = ?,
             verified_at = ?,
             updated_at = ?
         WHERE id = ?`
      ).run(gatewayOrderId, gatewayPaymentId, gatewaySignature, verifiedAt, verifiedAt, paymentOrderId);
      db.prepare(
        `INSERT INTO wallet_entries (id, user_id, type, status, amount, before_balance, after_balance, reference_id, note, created_at)
         VALUES (?, ?, 'DEPOSIT', 'SUCCESS', ?, ?, ?, ?, ?, ?)`
      ).run(
        `wallet_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
        existing.user_id,
        Number(existing.amount),
        currentBalance,
        nextBalance,
        gatewayPaymentId,
        `Razorpay payment ${gatewayPaymentId}`,
        verifiedAt
      );
    }
    db.exec("COMMIT");
  } catch (error) {
    db.exec("ROLLBACK");
    throw error;
  }

  return findPaymentOrderById(paymentOrderId);
}

export async function completePaymentLinkOrder({ reference, gatewayOrderId, gatewayPaymentId, gatewaySignature = "payment_link_webhook" }) {
  if (!reference) {
    return null;
  }

  const existingOrder = await findPaymentOrderByReference(reference);
  if (!existingOrder) {
    return null;
  }

  return completePaymentOrder({
    paymentOrderId: existingOrder.id,
    gatewayOrderId: gatewayOrderId || existingOrder.gatewayOrderId || `plink_${reference}`,
    gatewayPaymentId: gatewayPaymentId || existingOrder.gatewayPaymentId || `plinkpay_${reference}`,
    gatewaySignature
  });
}

export async function handlePaymentWebhook(reference, status) {
  if (isStandalonePostgresEnabled()) {
    const pool = getPgPool();
    const result = await pool.query(
      `UPDATE payment_orders
       SET status = $2, updated_at = $3
       WHERE reference = $1
       RETURNING id, user_id, provider, amount, status, reference, checkout_token, gateway_order_id, gateway_payment_id, gateway_signature, verified_at, redirect_url, created_at, updated_at`,
      [reference, status, nowIso()]
    );
    return mapPaymentOrderRow(result.rows[0]);
  }

  const db = getSqlite();
  db.prepare(`UPDATE payment_orders SET status = ?, updated_at = ? WHERE reference = ?`).run(status, nowIso(), reference);
  return mapPaymentOrderRow(
    db
      .prepare(
        `SELECT id, user_id, provider, amount, status, reference, checkout_token, gateway_order_id, gateway_payment_id, gateway_signature, verified_at, redirect_url, created_at, updated_at
         FROM payment_orders
         WHERE reference = ?
         LIMIT 1`
      )
      .get(reference)
  );
}

async function findWalletEntryById(entryId) {
  if (isStandalonePostgresEnabled()) {
    const pool = getPgPool();
    const result = await pool.query(
      `SELECT id, user_id, type, status, amount, before_balance, after_balance, reference_id, proof_url, note, created_at
         FROM wallet_entries
         WHERE id = $1
         LIMIT 1`,
      [entryId]
    );
    return mapWalletEntryRow(result.rows[0]);
  }

    return mapWalletEntryRow(
      getSqlite()
        .prepare(
          `SELECT id, user_id, type, status, amount, before_balance, after_balance, reference_id, proof_url, note, created_at
           FROM wallet_entries
           WHERE id = ?
           LIMIT 1`
        )
      .get(entryId)
  );
}

export async function findWalletEntryByReferenceId(userId, referenceId) {
  if (!referenceId) {
    return null;
  }

  if (isStandalonePostgresEnabled()) {
    const pool = getPgPool();
    const result = await pool.query(
      `SELECT id, user_id, type, status, amount, before_balance, after_balance, reference_id, proof_url, note, created_at
       FROM wallet_entries
       WHERE user_id = $1 AND reference_id = $2
       ORDER BY created_at DESC, id DESC
       LIMIT 1`,
      [userId, referenceId]
    );
    return mapWalletEntryRow(result.rows[0]);
  }

  return mapWalletEntryRow(
    getSqlite()
      .prepare(
        `SELECT id, user_id, type, status, amount, before_balance, after_balance, reference_id, proof_url, note, created_at
         FROM wallet_entries
         WHERE user_id = ? AND reference_id = ?
         ORDER BY created_at DESC, id DESC
         LIMIT 1`
      )
      .get(userId, referenceId)
  );
}

async function updateWalletEntryStatus(entryId, status) {
  if (isStandalonePostgresEnabled()) {
    const pool = getPgPool();
    await pool.query(`UPDATE wallet_entries SET status = $1 WHERE id = $2`, [status, entryId]);
  } else {
    getSqlite().prepare(`UPDATE wallet_entries SET status = ? WHERE id = ?`).run(status, entryId);
  }

  return findWalletEntryById(entryId);
}

export async function updateWalletEntryAdmin(entryId, updates = {}) {
  const current = await findWalletEntryById(entryId);
  if (!current) {
    return null;
  }

  const nextStatus = String(updates.status ?? current.status).trim() || current.status;
  const nextReferenceId = String(updates.referenceId ?? current.referenceId ?? "").trim();
  const nextProofUrl = String(updates.proofUrl ?? current.proofUrl ?? "").trim();
  const nextNote = String(updates.note ?? current.note ?? "").trim();

  if (isStandalonePostgresEnabled()) {
    const pool = getPgPool();
    const result = await pool.query(
      `UPDATE wallet_entries
       SET status = $2,
           reference_id = $3,
           proof_url = $4,
           note = $5
       WHERE id = $1
       RETURNING id, user_id, type, status, amount, before_balance, after_balance, reference_id, proof_url, note, created_at`,
      [entryId, nextStatus, nextReferenceId || null, nextProofUrl || null, nextNote || null]
    );
    return mapWalletEntryRow(result.rows[0]);
  }

  getSqlite()
    .prepare(
      `UPDATE wallet_entries
       SET status = ?, reference_id = ?, proof_url = ?, note = ?
       WHERE id = ?`
    )
    .run(nextStatus, nextReferenceId || null, nextProofUrl || null, nextNote || null, entryId);

  return findWalletEntryById(entryId);
}

export async function getWalletApprovalRequests() {
  const filters = ["DEPOSIT", "WITHDRAW"];

  if (isStandalonePostgresEnabled()) {
    const pool = await getReadyPgPool();
    const result = await pool.query(
      `SELECT id, user_id, type, status, amount, before_balance, after_balance, reference_id, proof_url, note, created_at
         FROM wallet_entries
         WHERE status = $1 AND type = ANY($2::text[])
         ORDER BY created_at DESC, id DESC`,
      ["INITIATED", filters]
    );
    return result.rows.map((row) => mapWalletEntryRow(row));
  }

    return getSqlite()
      .prepare(
        `SELECT id, user_id, type, status, amount, before_balance, after_balance, reference_id, proof_url, note, created_at
         FROM wallet_entries
         WHERE status = ? AND type IN (?, ?)
         ORDER BY created_at DESC, id DESC`
    )
    .all("INITIATED", filters[0], filters[1])
    .map((row) => mapWalletEntryRow(row));
}

export async function getWalletRequestHistory() {
  const filters = ["DEPOSIT", "WITHDRAW"];

  if (isStandalonePostgresEnabled()) {
    const pool = await getReadyPgPool();
    const result = await pool.query(
      `SELECT id, user_id, type, status, amount, before_balance, after_balance, reference_id, proof_url, note, created_at
         FROM wallet_entries
         WHERE type = ANY($1::text[])
         ORDER BY created_at DESC, id DESC`,
      [filters]
    );
    return result.rows.map((row) => mapWalletEntryRow(row));
  }

    return getSqlite()
      .prepare(
        `SELECT id, user_id, type, status, amount, before_balance, after_balance, reference_id, proof_url, note, created_at
         FROM wallet_entries
         WHERE type IN (?, ?)
         ORDER BY created_at DESC, id DESC`
    )
    .all(filters[0], filters[1])
    .map((row) => mapWalletEntryRow(row));
}

export async function resolveWalletApprovalRequest(entryId, action) {
  const request = await findWalletEntryById(entryId);
  if (!request || request.status !== "INITIATED" || !["DEPOSIT", "WITHDRAW"].includes(request.type)) {
    return null;
  }

  if (action === "reject") {
    return {
      request: await updateWalletEntryStatus(entryId, "REJECTED"),
      settlementEntry: null
    };
  }

  const beforeBalance = await getUserBalance(request.userId);
  if (request.type === "WITHDRAW" && request.amount > beforeBalance) {
    throw new Error("User has insufficient live balance for withdraw approval");
  }

  const settlementEntry = await addWalletEntry({
    userId: request.userId,
    type: request.type,
    status: "SUCCESS",
    amount: request.amount,
    beforeBalance,
    afterBalance: request.type === "DEPOSIT" ? beforeBalance + request.amount : beforeBalance - request.amount
  });

  return {
    request: await updateWalletEntryStatus(entryId, "BACKOFFICE"),
    settlementEntry
  };
}

export async function updateUserApprovalStatus(userId, status) {
  const current = await findUserById(userId);
  if (!current) {
    return null;
  }

  const approvedAt = status === "Approved" ? nowIso() : null;
  const rejectedAt = status === "Rejected" ? nowIso() : null;
  const signupBonusGranted = status === "Approved" ? current.signupBonusGranted || true : current.signupBonusGranted;

  if (isStandalonePostgresEnabled()) {
    const pool = await getReadyPgPool();
    await pool.query(
      `UPDATE users
       SET approval_status = $1, approved_at = $2, rejected_at = $3, signup_bonus_granted = $4
       WHERE id = $5`,
      [status, approvedAt, rejectedAt, signupBonusGranted, userId]
    );
  } else {
    getSqlite()
      .prepare(
        `UPDATE users
         SET approval_status = ?, approved_at = ?, rejected_at = ?, signup_bonus_granted = ?
         WHERE id = ?`
      )
      .run(status, approvedAt, rejectedAt, signupBonusGranted ? 1 : 0, userId);
  }

  if (status === "Approved" && !current.signupBonusGranted) {
    const beforeBalance = await getUserBalance(userId);
    await addWalletEntry({
      userId,
      type: "SIGNUP_BONUS",
      status: "SUCCESS",
      amount: signupBonusAmount,
      beforeBalance,
      afterBalance: beforeBalance + signupBonusAmount
    });
  }

  return findUserById(userId);
}

export async function addAuditLog(entry) {
  const id = `audit_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
  const createdAt = nowIso();

  if (isStandalonePostgresEnabled()) {
    const pool = getPgPool();
    await pool.query(
      `INSERT INTO audit_logs (id, actor_user_id, action, entity_type, entity_id, details, created_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7)`,
      [id, entry.actorUserId, entry.action, entry.entityType, entry.entityId, entry.details, createdAt]
    );
  } else {
    getSqlite()
      .prepare(
        `INSERT INTO audit_logs (id, actor_user_id, action, entity_type, entity_id, details, created_at)
         VALUES (?, ?, ?, ?, ?, ?, ?)`
      )
      .run(id, entry.actorUserId, entry.action, entry.entityType, entry.entityId, entry.details, createdAt);
  }

  return { id, createdAt, ...entry };
}

export async function getAuditLogs(limit = 100) {
  if (isStandalonePostgresEnabled()) {
    const pool = await getReadyPgPool();
    const result = await pool.query(
      `SELECT id, actor_user_id, action, entity_type, entity_id, details, created_at
       FROM audit_logs
       ORDER BY created_at DESC, id DESC
       LIMIT $1`,
      [limit]
    );
    return result.rows.map((row) => mapAuditLogRow(row));
  }

  return getSqlite()
    .prepare(
      `SELECT id, actor_user_id, action, entity_type, entity_id, details, created_at
       FROM audit_logs
       ORDER BY created_at DESC, id DESC
       LIMIT ?`
    )
    .all(limit)
    .map((row) => mapAuditLogRow(row));
}

export async function getAdminSnapshot() {
  if (isStandalonePostgresEnabled()) {
    const pool = await getReadyPgPool();
    const [usersResult, sessionsResult, walletResult, bidsResult, marketsResult, devicesResult] = await Promise.all([
      pool.query(`SELECT id, phone, password_hash, mpin_hash, name, joined_at, referral_code, role, approval_status, approved_at, rejected_at, blocked_at, deactivated_at, status_note, signup_bonus_granted, referred_by_user_id FROM users ORDER BY joined_at DESC, id DESC`),
      pool.query(`SELECT token_hash, user_id, created_at FROM sessions ORDER BY created_at DESC, token_hash DESC`),
      pool.query(`SELECT id, user_id, type, status, amount, before_balance, after_balance, created_at FROM wallet_entries ORDER BY created_at DESC, id DESC`),
      pool.query(`SELECT id, user_id, market, board_label, session_type, digit, points, status, payout, settled_at, settled_result, created_at FROM bids ORDER BY created_at DESC, id DESC`),
      pool.query(`SELECT id, slug, name, result, status, action, open_time, close_time, category FROM markets ORDER BY id ASC`),
      pool.query(`SELECT id, user_id, platform, token, enabled, created_at, updated_at FROM notification_devices ORDER BY created_at DESC, id DESC`)
    ]);

    return {
      users: usersResult.rows.map((row) => mapUserRow(row)),
      sessions: sessionsResult.rows.map((row) => ({ tokenHash: row.token_hash, userId: row.user_id, createdAt: toIso(row.created_at) })),
      walletEntries: walletResult.rows.map((row) => mapWalletEntryRow(row)),
      bids: bidsResult.rows.map((row) => mapBidRow(row)),
      markets: marketsResult.rows.map((row) => mapMarketRow(row)),
      notificationDevices: devicesResult.rows.map((row) => mapNotificationDeviceRow(row))
    };
  }

  const db = getSqlite();
  return {
    users: db
      .prepare(`SELECT id, phone, password_hash, mpin_hash, name, joined_at, referral_code, role, approval_status, approved_at, rejected_at, blocked_at, deactivated_at, status_note, signup_bonus_granted, referred_by_user_id FROM users ORDER BY joined_at DESC, id DESC`)
      .all()
      .map((row) => mapUserRow(row)),
    sessions: db
      .prepare(`SELECT token_hash, user_id, created_at FROM sessions ORDER BY created_at DESC, token_hash DESC`)
      .all()
      .map((row) => ({ tokenHash: row.token_hash, userId: row.user_id, createdAt: toIso(row.created_at) })),
    walletEntries: db
      .prepare(`SELECT id, user_id, type, status, amount, before_balance, after_balance, created_at FROM wallet_entries ORDER BY created_at DESC, id DESC`)
      .all()
      .map((row) => mapWalletEntryRow(row)),
    bids: db
      .prepare(`SELECT id, user_id, market, board_label, session_type, digit, points, status, payout, settled_at, settled_result, created_at FROM bids ORDER BY created_at DESC, id DESC`)
      .all()
      .map((row) => mapBidRow(row)),
    markets: db
      .prepare(`SELECT id, slug, name, result, status, action, open_time, close_time, category FROM markets ORDER BY id ASC`)
      .all()
      .map((row) => mapMarketRow(row)),
    notificationDevices: db
      .prepare(`SELECT id, user_id, platform, token, enabled, created_at, updated_at FROM notification_devices ORDER BY created_at DESC, id DESC`)
      .all()
      .map((row) => mapNotificationDeviceRow(row))
  };
}

export { hashCredential, verifyCredential };
