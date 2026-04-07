import { mkdirSync } from "node:fs";
import path from "node:path";
import { createRequire } from "node:module";
import { Pool } from "pg";
import { chartRows, markets as marketSeed } from "@/data/mock";
import { isPostgresEnabled, productionConfig } from "@/services/backend-service/core/config";
import { createOpaqueToken, hashCredential, hashSecret, verifyCredential } from "@/services/backend-service/core/security";
import {
  AuditLog,
  BankAccount,
  Bid,
  ChartRecord,
  Database,
  Market,
  NotificationDevice,
  NotificationRecord,
  OtpChallenge,
  PaymentOrder,
  Session,
  User,
  WalletLedgerEntry
} from "@/services/backend-service/core/schema";

const now = () => new Date().toISOString();
const dbFilePath = path.join(process.cwd(), "backend", "data", "server.db");
const signupBonusAmount = 25;

const defaultUser: User = {
  id: "user_1",
  phone: "9309782081",
  passwordHash: hashSecret("demo1234"),
  mpinHash: hashSecret("1234"),
  name: "Siddhant Borkar",
  joinedAt: "2025-04-12T10:00:00.000Z",
  referralCode: "621356",
  role: "admin",
  approvalStatus: "Approved",
  approvedAt: "2025-04-12T10:00:00.000Z",
  rejectedAt: null,
  signupBonusGranted: true,
  referredByUserId: null
};

const defaultSessionToken = "demo-session-token";
const sessionTtlMs = productionConfig.sessionTtlHours * 60 * 60 * 1000;

const marketsSeed: Market[] = marketSeed.map((market, index) => ({
  id: `market_${index + 1}`,
  slug: market.name.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, ""),
  name: market.name,
  result: market.result,
  status: market.status,
  action: market.action,
  open: market.open,
  close: market.close,
  category: market.category
}));

const chartsSeed: ChartRecord[] = marketsSeed.flatMap((market) => [
  { marketSlug: market.slug, chartType: "jodi", rows: chartRows.map((row) => row.slice(1)) },
  { marketSlug: market.slug, chartType: "panna", rows: chartRows }
]);

type SqliteDatabase = {
  exec: (sql: string) => void;
  prepare: (sql: string) => {
    run: (...params: unknown[]) => unknown;
    get: (...params: unknown[]) => unknown;
    all: (...params: unknown[]) => unknown[];
  };
};

let sqlite: SqliteDatabase | null = null;
let pgPool: Pool | null = null;
let pgReady: Promise<void> | null = null;
const runtimeRequire =
  typeof require === "function"
    ? require
    : createRequire(path.join(process.cwd(), "__backend-runtime__.cjs"));

function toIsoString(value: unknown): string {
  if (value instanceof Date) {
    return value.toISOString();
  }
  if (typeof value === "string") {
    return value;
  }
  if (typeof value === "number") {
    return new Date(value).toISOString();
  }
  return String(value ?? "");
}

function toNullableIsoString(value: unknown): string | null {
  if (value == null) {
    return null;
  }
  return toIsoString(value);
}

function createSqliteDatabase(filePath: string): SqliteDatabase {
  const { DatabaseSync } = runtimeRequire("node:sqlite");
  return new DatabaseSync(filePath);
}

function mapUser(row: {
  id: string;
  phone: string;
  password_hash: string;
  mpin_hash: string;
  name: string;
  joined_at: string | Date;
  referral_code: string;
  role?: "admin" | "user";
  approval_status?: "Pending" | "Approved" | "Rejected";
  approved_at?: string | Date | null;
  rejected_at?: string | Date | null;
  signup_bonus_granted?: boolean | number;
  referred_by_user_id?: string | null;
}): User {
  return {
    id: row.id,
    phone: row.phone,
    passwordHash: row.password_hash,
    mpinHash: row.mpin_hash,
    name: row.name,
    joinedAt: toIsoString(row.joined_at),
    referralCode: row.referral_code,
    role: row.role === "admin" ? "admin" : "user",
    approvalStatus: row.approval_status === "Rejected" ? "Rejected" : row.approval_status === "Approved" ? "Approved" : "Pending",
    approvedAt: toNullableIsoString(row.approved_at),
    rejectedAt: toNullableIsoString(row.rejected_at),
    signupBonusGranted: Boolean(row.signup_bonus_granted),
    referredByUserId: row.referred_by_user_id ?? null
  };
}

function mapSession(row: { token_hash: string; user_id: string; created_at: string | Date }): Session {
  return {
    tokenHash: row.token_hash,
    userId: row.user_id,
    createdAt: toIsoString(row.created_at)
  };
}

function mapOtpChallenge(row: {
  id: string;
  phone: string;
  code_hash: string;
  purpose: OtpChallenge["purpose"];
  expires_at: string | Date;
  consumed_at?: string | Date | null;
  created_at: string | Date;
}): OtpChallenge {
  return {
    id: row.id,
    phone: row.phone,
    codeHash: row.code_hash,
    purpose: row.purpose,
    expiresAt: toIsoString(row.expires_at),
    consumedAt: toNullableIsoString(row.consumed_at),
    createdAt: toIsoString(row.created_at)
  };
}

function mapWalletEntry(row: {
  id: string;
  user_id: string;
  type: WalletLedgerEntry["type"];
  status: WalletLedgerEntry["status"];
  amount: number | string;
  before_balance: number | string;
  after_balance: number | string;
  created_at: string | Date;
}): WalletLedgerEntry {
  return {
    id: row.id,
    userId: row.user_id,
    type: row.type,
    status: row.status,
    amount: Number(row.amount),
    beforeBalance: Number(row.before_balance),
    afterBalance: Number(row.after_balance),
    createdAt: toIsoString(row.created_at)
  };
}

function mapBid(row: {
  id: string;
  user_id: string;
  market: string;
  board_label: string;
  session_type?: "Open" | "Close";
  digit: string;
  points: number | string;
  status: Bid["status"];
  payout?: number | string;
  settled_at?: string | Date | null;
  settled_result?: string | null;
  created_at: string | Date;
}): Bid {
  return {
    id: row.id,
    userId: row.user_id,
    market: row.market,
    boardLabel: row.board_label,
    sessionType: row.session_type === "Open" ? "Open" : "Close",
    digit: row.digit,
    points: Number(row.points),
    status: row.status,
    payout: Number(row.payout ?? 0),
    settledAt: toNullableIsoString(row.settled_at),
    settledResult: row.settled_result ?? null,
    createdAt: toIsoString(row.created_at)
  };
}

function mapBankAccount(row: {
  id: string;
  user_id: string;
  account_number: string;
  holder_name: string;
  ifsc: string;
  created_at: string | Date;
}): BankAccount {
  return {
    id: row.id,
    userId: row.user_id,
    accountNumber: row.account_number,
    holderName: row.holder_name,
    ifsc: row.ifsc,
    createdAt: toIsoString(row.created_at)
  };
}

function mapMarket(row: {
  id: string;
  slug: string;
  name: string;
  result: string;
  status: string;
  action: string;
  open_time: string;
  close_time: string;
  category: Market["category"];
}): Market {
  return {
    id: row.id,
    slug: row.slug,
    name: row.name,
    result: row.result,
    status: row.status,
    action: row.action,
    open: row.open_time,
    close: row.close_time,
    category: row.category
  };
}

function mapChart(row: { market_slug: string; chart_type: ChartRecord["chartType"]; rows_json: string | string[][] }): ChartRecord {
  return {
    marketSlug: row.market_slug,
    chartType: row.chart_type,
    rows: Array.isArray(row.rows_json) ? row.rows_json : (JSON.parse(row.rows_json) as string[][])
  };
}

function mapAuditLog(row: {
  id: string;
  actor_user_id: string;
  action: string;
  entity_type: string;
  entity_id: string;
  details: string;
  created_at: string | Date;
}): AuditLog {
  return {
    id: row.id,
    actorUserId: row.actor_user_id,
    action: row.action,
    entityType: row.entity_type,
    entityId: row.entity_id,
    details: row.details,
    createdAt: toIsoString(row.created_at)
  };
}

function mapNotificationDevice(row: {
  id: string;
  user_id: string;
  platform: "android" | "ios" | "web";
  token: string;
  enabled: boolean | number;
  created_at: string | Date;
  updated_at: string | Date;
}): NotificationDevice {
  return {
    id: row.id,
    userId: row.user_id,
    platform: row.platform,
    token: row.token,
    enabled: Boolean(row.enabled),
    createdAt: toIsoString(row.created_at),
    updatedAt: toIsoString(row.updated_at)
  };
}

function mapNotificationRecord(row: {
  id: string;
  user_id: string;
  title: string;
  body: string;
  channel: NotificationRecord["channel"];
  read: boolean | number;
  created_at: string | Date;
}): NotificationRecord {
  return {
    id: row.id,
    userId: row.user_id,
    title: row.title,
    body: row.body,
    channel: row.channel,
    read: Boolean(row.read),
    createdAt: toIsoString(row.created_at)
  };
}

function mapPaymentOrder(row: {
  id: string;
  user_id: string;
  provider: PaymentOrder["provider"];
  amount: number | string;
  status: PaymentOrder["status"];
  reference: string;
  redirect_url: string | null;
  created_at: string | Date;
  updated_at: string | Date;
}): PaymentOrder {
  return {
    id: row.id,
    userId: row.user_id,
    provider: row.provider,
    amount: Number(row.amount),
    status: row.status,
    reference: row.reference,
    redirectUrl: row.redirect_url,
    createdAt: toIsoString(row.created_at),
    updatedAt: toIsoString(row.updated_at)
  };
}

function ensureColumn(db: SqliteDatabase, table: string, column: string, definition: string) {
  const rows = db.prepare(`PRAGMA table_info(${table})`).all() as Array<{ name: string }>;
  if (!rows.some((row) => row.name === column)) {
    db.exec(`ALTER TABLE ${table} ADD COLUMN ${column} ${definition}`);
  }
}

function seedSqlite(db: SqliteDatabase) {
  const usersCount = Number((db.prepare("SELECT COUNT(*) AS count FROM users").get() as { count: number }).count);
  if (usersCount === 0) {
    db.prepare(
      `INSERT INTO users (id, phone, password_hash, mpin_hash, name, joined_at, referral_code, role, approval_status, approved_at, rejected_at, signup_bonus_granted, referred_by_user_id)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
    ).run(
      defaultUser.id,
      defaultUser.phone,
      defaultUser.passwordHash,
      defaultUser.mpinHash,
      defaultUser.name,
      defaultUser.joinedAt,
      defaultUser.referralCode,
      defaultUser.role,
      defaultUser.approvalStatus,
      defaultUser.approvedAt,
      defaultUser.rejectedAt,
      defaultUser.signupBonusGranted ? 1 : 0,
      defaultUser.referredByUserId
    );
  } else {
    db.prepare(
      "UPDATE users SET role = ?, approval_status = ?, approved_at = ?, rejected_at = ?, signup_bonus_granted = ?, referred_by_user_id = ? WHERE phone = ?"
    ).run(
      defaultUser.role,
      defaultUser.approvalStatus,
      defaultUser.approvedAt,
      defaultUser.rejectedAt,
      defaultUser.signupBonusGranted ? 1 : 0,
      defaultUser.referredByUserId,
      defaultUser.phone
    );
  }

  const sessionsCount = Number((db.prepare("SELECT COUNT(*) AS count FROM sessions").get() as { count: number }).count);
  if (sessionsCount === 0) {
    db.prepare("INSERT INTO sessions (token_hash, user_id, created_at) VALUES (?, ?, ?)").run(
      hashSecret(defaultSessionToken),
      defaultUser.id,
      now()
    );
  }

  const walletCount = Number((db.prepare("SELECT COUNT(*) AS count FROM wallet_entries").get() as { count: number }).count);
  if (walletCount === 0) {
    db.prepare(
      `INSERT INTO wallet_entries (id, user_id, type, status, amount, before_balance, after_balance, created_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
    ).run("wallet_1", defaultUser.id, "DEPOSIT", "SUCCESS", 2000, 0, 2000, now());
  }

  const marketCount = Number((db.prepare("SELECT COUNT(*) AS count FROM markets").get() as { count: number }).count);
  if (marketCount === 0) {
    const insertMarket = db.prepare(
      `INSERT INTO markets (id, slug, name, result, status, action, open_time, close_time, category)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
    );
    for (const market of marketsSeed) {
      insertMarket.run(market.id, market.slug, market.name, market.result, market.status, market.action, market.open, market.close, market.category);
    }
  }

  const chartCount = Number((db.prepare("SELECT COUNT(*) AS count FROM charts").get() as { count: number }).count);
  if (chartCount === 0) {
    const insertChart = db.prepare("INSERT INTO charts (market_slug, chart_type, rows_json) VALUES (?, ?, ?)");
    for (const chart of chartsSeed) {
      insertChart.run(chart.marketSlug, chart.chartType, JSON.stringify(chart.rows));
    }
  }
}

function getSqlite() {
  if (sqlite) {
    return sqlite;
  }

  mkdirSync(path.dirname(dbFilePath), { recursive: true });
  sqlite = createSqliteDatabase(dbFilePath);
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
      signup_bonus_granted INTEGER NOT NULL DEFAULT 0,
      referred_by_user_id TEXT
    );

    CREATE TABLE IF NOT EXISTS sessions (
      token_hash TEXT PRIMARY KEY,
      user_id TEXT NOT NULL,
      created_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS otp_challenges (
      id TEXT PRIMARY KEY,
      phone TEXT NOT NULL,
      code_hash TEXT NOT NULL,
      purpose TEXT NOT NULL,
      expires_at TEXT NOT NULL,
      consumed_at TEXT,
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
      redirect_url TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );
  `);

  ensureColumn(sqlite, "users", "role", "TEXT NOT NULL DEFAULT 'user'");
  ensureColumn(sqlite, "users", "approval_status", "TEXT NOT NULL DEFAULT 'Approved'");
  ensureColumn(sqlite, "users", "approved_at", "TEXT");
  ensureColumn(sqlite, "users", "rejected_at", "TEXT");
  ensureColumn(sqlite, "users", "signup_bonus_granted", "INTEGER NOT NULL DEFAULT 0");
  ensureColumn(sqlite, "users", "referred_by_user_id", "TEXT");
  ensureColumn(sqlite, "bids", "payout", "REAL NOT NULL DEFAULT 0");
  ensureColumn(sqlite, "bids", "settled_at", "TEXT");
  ensureColumn(sqlite, "bids", "settled_result", "TEXT");
  ensureColumn(sqlite, "bids", "session_type", "TEXT NOT NULL DEFAULT 'Close'");
  seedSqlite(sqlite);
  return sqlite;
}

function getPgPool() {
  if (!productionConfig.databaseUrl) {
    throw new Error("DATABASE_URL is required for postgres mode.");
  }

  if (!pgPool) {
    const normalizedUrl = new URL(productionConfig.databaseUrl);
    normalizedUrl.searchParams.delete("sslmode");

    pgPool = new Pool({
      connectionString: normalizedUrl.toString(),
      ssl: { rejectUnauthorized: false }
    });
  }

  return pgPool;
}

async function ensurePostgresReady() {
  if (!isPostgresEnabled()) {
    return;
  }

  if (!pgReady) {
    pgReady = (async () => {
      const pool = getPgPool();
      await pool.query(`
        CREATE TABLE IF NOT EXISTS users (
          id TEXT PRIMARY KEY,
          phone TEXT UNIQUE NOT NULL,
          password_hash TEXT NOT NULL,
          mpin_hash TEXT NOT NULL,
          name TEXT NOT NULL,
          joined_at TIMESTAMPTZ NOT NULL,
          referral_code TEXT NOT NULL,
          role TEXT NOT NULL DEFAULT 'user',
          approval_status TEXT NOT NULL DEFAULT 'Approved',
          approved_at TIMESTAMPTZ,
          rejected_at TIMESTAMPTZ,
          signup_bonus_granted BOOLEAN NOT NULL DEFAULT FALSE,
          referred_by_user_id TEXT REFERENCES users(id)
        );

        CREATE TABLE IF NOT EXISTS sessions (
          token_hash TEXT PRIMARY KEY,
          user_id TEXT NOT NULL REFERENCES users(id),
          created_at TIMESTAMPTZ NOT NULL
        );

        CREATE TABLE IF NOT EXISTS otp_challenges (
          id TEXT PRIMARY KEY,
          phone TEXT NOT NULL,
          code_hash TEXT NOT NULL,
          purpose TEXT NOT NULL,
          expires_at TIMESTAMPTZ NOT NULL,
          consumed_at TIMESTAMPTZ,
          created_at TIMESTAMPTZ NOT NULL
        );

        CREATE TABLE IF NOT EXISTS wallet_entries (
          id TEXT PRIMARY KEY,
          user_id TEXT NOT NULL REFERENCES users(id),
          type TEXT NOT NULL,
          status TEXT NOT NULL,
          amount NUMERIC(12,2) NOT NULL,
          before_balance NUMERIC(12,2) NOT NULL,
          after_balance NUMERIC(12,2) NOT NULL,
          created_at TIMESTAMPTZ NOT NULL
        );

        CREATE TABLE IF NOT EXISTS bids (
          id TEXT PRIMARY KEY,
          user_id TEXT NOT NULL REFERENCES users(id),
          market TEXT NOT NULL,
          board_label TEXT NOT NULL,
          session_type TEXT NOT NULL,
          digit TEXT NOT NULL,
          points NUMERIC(12,2) NOT NULL,
          status TEXT NOT NULL,
          payout NUMERIC(12,2) NOT NULL DEFAULT 0,
          settled_at TIMESTAMPTZ,
          settled_result TEXT,
          created_at TIMESTAMPTZ NOT NULL
        );

        CREATE TABLE IF NOT EXISTS bank_accounts (
          id TEXT PRIMARY KEY,
          user_id TEXT NOT NULL REFERENCES users(id),
          account_number TEXT NOT NULL,
          holder_name TEXT NOT NULL,
          ifsc TEXT NOT NULL,
          created_at TIMESTAMPTZ NOT NULL
        );

        CREATE TABLE IF NOT EXISTS markets (
          id TEXT PRIMARY KEY,
          slug TEXT UNIQUE NOT NULL,
          name TEXT NOT NULL,
          result TEXT NOT NULL,
          status TEXT NOT NULL,
          action TEXT NOT NULL,
          open_time TEXT NOT NULL,
          close_time TEXT NOT NULL,
          category TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS charts (
          market_slug TEXT NOT NULL REFERENCES markets(slug),
          chart_type TEXT NOT NULL,
          rows_json JSONB NOT NULL,
          PRIMARY KEY (market_slug, chart_type)
        );

        CREATE TABLE IF NOT EXISTS audit_logs (
          id TEXT PRIMARY KEY,
          actor_user_id TEXT NOT NULL REFERENCES users(id),
          action TEXT NOT NULL,
          entity_type TEXT NOT NULL,
          entity_id TEXT NOT NULL,
          details TEXT NOT NULL,
          created_at TIMESTAMPTZ NOT NULL
        );

        CREATE TABLE IF NOT EXISTS notification_devices (
          id TEXT PRIMARY KEY,
          user_id TEXT NOT NULL REFERENCES users(id),
          platform TEXT NOT NULL,
          token TEXT NOT NULL,
          enabled BOOLEAN NOT NULL DEFAULT TRUE,
          created_at TIMESTAMPTZ NOT NULL,
          updated_at TIMESTAMPTZ NOT NULL,
          UNIQUE (user_id, token)
        );

        CREATE TABLE IF NOT EXISTS notifications (
          id TEXT PRIMARY KEY,
          user_id TEXT NOT NULL REFERENCES users(id),
          title TEXT NOT NULL,
          body TEXT NOT NULL,
          channel TEXT NOT NULL,
          read BOOLEAN NOT NULL DEFAULT FALSE,
          created_at TIMESTAMPTZ NOT NULL
        );

        CREATE TABLE IF NOT EXISTS payment_orders (
          id TEXT PRIMARY KEY,
          user_id TEXT NOT NULL REFERENCES users(id),
          provider TEXT NOT NULL,
          amount NUMERIC(12,2) NOT NULL,
          status TEXT NOT NULL,
          reference TEXT UNIQUE NOT NULL,
          redirect_url TEXT,
          created_at TIMESTAMPTZ NOT NULL,
          updated_at TIMESTAMPTZ NOT NULL
        );
      `);

      await pool.query(
        `INSERT INTO users (id, phone, password_hash, mpin_hash, name, joined_at, referral_code, role, approval_status, approved_at, rejected_at, signup_bonus_granted, referred_by_user_id)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
         ON CONFLICT (id) DO UPDATE SET role = EXCLUDED.role, phone = EXCLUDED.phone, approval_status = EXCLUDED.approval_status, approved_at = EXCLUDED.approved_at, signup_bonus_granted = EXCLUDED.signup_bonus_granted`,
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
          defaultUser.approvedAt,
          defaultUser.rejectedAt,
          defaultUser.signupBonusGranted,
          defaultUser.referredByUserId
        ]
      );

      await pool.query(
        `INSERT INTO sessions (token_hash, user_id, created_at)
         VALUES ($1, $2, $3)
         ON CONFLICT (token_hash) DO NOTHING`,
        [hashSecret(defaultSessionToken), defaultUser.id, now()]
      );

      await pool.query(
        `INSERT INTO wallet_entries (id, user_id, type, status, amount, before_balance, after_balance, created_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
         ON CONFLICT (id) DO NOTHING`,
        ["wallet_1", defaultUser.id, "DEPOSIT", "SUCCESS", 2000, 0, 2000, now()]
      );

      for (const market of marketsSeed) {
        await pool.query(
          `INSERT INTO markets (id, slug, name, result, status, action, open_time, close_time, category)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
           ON CONFLICT (slug) DO NOTHING`,
          [market.id, market.slug, market.name, market.result, market.status, market.action, market.open, market.close, market.category]
        );
      }

      for (const chart of chartsSeed) {
        await pool.query(
          `INSERT INTO charts (market_slug, chart_type, rows_json)
           VALUES ($1, $2, $3::jsonb)
           ON CONFLICT (market_slug, chart_type) DO NOTHING`,
          [chart.marketSlug, chart.chartType, JSON.stringify(chart.rows)]
        );
      }
    })();
  }

  await pgReady;
}

async function getUsers() {
  if (!isPostgresEnabled()) {
    const db = getSqlite();
    return (db.prepare("SELECT * FROM users").all() as Array<Record<string, unknown>>).map((row) => mapUser(row as never));
  }
  await ensurePostgresReady();
  const result = await getPgPool().query("SELECT * FROM users");
  return result.rows.map((row: Record<string, unknown>) => mapUser(row as never));
}

async function getSessions() {
  if (!isPostgresEnabled()) {
    const db = getSqlite();
    return (db.prepare("SELECT * FROM sessions ORDER BY created_at DESC").all() as Array<Record<string, unknown>>).map((row) => mapSession(row as never));
  }
  await ensurePostgresReady();
  const result = await getPgPool().query("SELECT * FROM sessions ORDER BY created_at DESC");
  return result.rows.map((row: Record<string, unknown>) => mapSession(row as never));
}

async function getWalletEntries() {
  if (!isPostgresEnabled()) {
    const db = getSqlite();
    return (db.prepare("SELECT * FROM wallet_entries ORDER BY created_at DESC, id DESC").all() as Array<Record<string, unknown>>).map((row) =>
      mapWalletEntry(row as never)
    );
  }
  await ensurePostgresReady();
  const result = await getPgPool().query("SELECT * FROM wallet_entries ORDER BY created_at DESC, id DESC");
  return result.rows.map((row: Record<string, unknown>) => mapWalletEntry(row as never));
}

async function getOtpChallenges() {
  if (!isPostgresEnabled()) {
    const db = getSqlite();
    return (db.prepare("SELECT * FROM otp_challenges ORDER BY created_at DESC, id DESC").all() as Array<Record<string, unknown>>).map((row) =>
      mapOtpChallenge(row as never)
    );
  }
  await ensurePostgresReady();
  const result = await getPgPool().query("SELECT * FROM otp_challenges ORDER BY created_at DESC, id DESC");
  return result.rows.map((row: Record<string, unknown>) => mapOtpChallenge(row as never));
}

async function getBids() {
  if (!isPostgresEnabled()) {
    const db = getSqlite();
    return (db.prepare("SELECT * FROM bids ORDER BY created_at DESC, id DESC").all() as Array<Record<string, unknown>>).map((row) => mapBid(row as never));
  }
  await ensurePostgresReady();
  const result = await getPgPool().query("SELECT * FROM bids ORDER BY created_at DESC, id DESC");
  return result.rows.map((row: Record<string, unknown>) => mapBid(row as never));
}

async function getBankAccounts() {
  if (!isPostgresEnabled()) {
    const db = getSqlite();
    return (db.prepare("SELECT * FROM bank_accounts ORDER BY created_at DESC, id DESC").all() as Array<Record<string, unknown>>).map((row) =>
      mapBankAccount(row as never)
    );
  }
  await ensurePostgresReady();
  const result = await getPgPool().query("SELECT * FROM bank_accounts ORDER BY created_at DESC, id DESC");
  return result.rows.map((row: Record<string, unknown>) => mapBankAccount(row as never));
}

async function getMarkets() {
  if (!isPostgresEnabled()) {
    const db = getSqlite();
    return (db.prepare("SELECT * FROM markets ORDER BY id ASC").all() as Array<Record<string, unknown>>).map((row) => mapMarket(row as never));
  }
  await ensurePostgresReady();
  const result = await getPgPool().query("SELECT * FROM markets ORDER BY id ASC");
  return result.rows.map((row: Record<string, unknown>) => mapMarket(row as never));
}

async function getCharts() {
  if (!isPostgresEnabled()) {
    const db = getSqlite();
    return (db.prepare("SELECT * FROM charts").all() as Array<Record<string, unknown>>).map((row) => mapChart(row as never));
  }
  await ensurePostgresReady();
  const result = await getPgPool().query("SELECT market_slug, chart_type, rows_json::text AS rows_json FROM charts");
  return result.rows.map((row: Record<string, unknown>) => mapChart(row as never));
}

async function getNotificationDevices() {
  if (!isPostgresEnabled()) {
    const db = getSqlite();
    return (db.prepare("SELECT * FROM notification_devices ORDER BY created_at DESC, id DESC").all() as Array<Record<string, unknown>>).map((row) =>
      mapNotificationDevice(row as never)
    );
  }
  await ensurePostgresReady();
  const result = await getPgPool().query("SELECT * FROM notification_devices ORDER BY created_at DESC, id DESC");
  return result.rows.map((row: Record<string, unknown>) => mapNotificationDevice(row as never));
}

async function getNotifications() {
  if (!isPostgresEnabled()) {
    const db = getSqlite();
    return (db.prepare("SELECT * FROM notifications ORDER BY created_at DESC, id DESC").all() as Array<Record<string, unknown>>).map((row) =>
      mapNotificationRecord(row as never)
    );
  }
  await ensurePostgresReady();
  const result = await getPgPool().query("SELECT * FROM notifications ORDER BY created_at DESC, id DESC");
  return result.rows.map((row: Record<string, unknown>) => mapNotificationRecord(row as never));
}

async function getPaymentOrders() {
  if (!isPostgresEnabled()) {
    const db = getSqlite();
    return (db.prepare("SELECT * FROM payment_orders ORDER BY created_at DESC, id DESC").all() as Array<Record<string, unknown>>).map((row) =>
      mapPaymentOrder(row as never)
    );
  }
  await ensurePostgresReady();
  const result = await getPgPool().query("SELECT * FROM payment_orders ORDER BY created_at DESC, id DESC");
  return result.rows.map((row: Record<string, unknown>) => mapPaymentOrder(row as never));
}

async function getAuditLogs() {
  if (!isPostgresEnabled()) {
    const db = getSqlite();
    return (db.prepare("SELECT * FROM audit_logs ORDER BY created_at DESC, id DESC").all() as Array<Record<string, unknown>>).map((row) =>
      mapAuditLog(row as never)
    );
  }
  await ensurePostgresReady();
  const result = await getPgPool().query("SELECT * FROM audit_logs ORDER BY created_at DESC, id DESC");
  return result.rows.map((row: Record<string, unknown>) => mapAuditLog(row as never));
}

export async function getDb(): Promise<Database> {
  const [users, sessions, otpChallenges, walletEntries, bids, bankAccounts, markets, charts, notificationDevices, notifications, paymentOrders, auditLogs] =
    await Promise.all([
      getUsers(),
      getSessions(),
      getOtpChallenges(),
      getWalletEntries(),
      getBids(),
      getBankAccounts(),
      getMarkets(),
      getCharts(),
      getNotificationDevices(),
      getNotifications(),
      getPaymentOrders(),
      getAuditLogs()
    ]);

  return {
    users,
    sessions,
    otpChallenges,
    walletEntries,
    bids,
    bankAccounts,
    markets,
    charts,
    notificationDevices,
    notifications,
    paymentOrders,
    auditLogs
  };
}

export async function createSession(userId: string) {
  const rawToken = createOpaqueToken();
  const session: Session = {
    tokenHash: hashSecret(rawToken),
    userId,
    createdAt: now()
  };

  if (!isPostgresEnabled()) {
    const db = getSqlite();
    db.prepare("INSERT INTO sessions (token_hash, user_id, created_at) VALUES (?, ?, ?)").run(session.tokenHash, session.userId, session.createdAt);
    return { rawToken, session };
  }

  await ensurePostgresReady();
  await getPgPool().query("INSERT INTO sessions (token_hash, user_id, created_at) VALUES ($1, $2, $3)", [
    session.tokenHash,
    session.userId,
    session.createdAt
  ]);

  return { rawToken, session };
}

async function generateReferralCode() {
  let next = String(Math.floor(100000 + Math.random() * 900000));

  while (await hasReferralCode(next)) {
    next = String(Math.floor(100000 + Math.random() * 900000));
  }

  return next;
}

export async function createUserAccount(input: { phone: string; passwordHash: string; referenceCode: string; name?: string }) {
  const phone = input.phone.trim();
  const referenceCode = input.referenceCode.trim();

  if (await findUserByPhone(phone)) {
    return { user: null, error: "Phone number already registered" } as const;
  }

  const referrer = await findUserByReferralCode(referenceCode);
  if (!referrer) {
    return { user: null, error: "Invalid reference code" } as const;
  }

  const user: User = {
    id: `user_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
    phone,
    passwordHash: input.passwordHash,
    mpinHash: hashCredential("0000"),
    name: input.name?.trim() || `Player ${phone.slice(-4)}`,
    joinedAt: now(),
    referralCode: await generateReferralCode(),
    role: "user",
    approvalStatus: "Pending",
    approvedAt: null,
    rejectedAt: null,
    signupBonusGranted: false,
    referredByUserId: referrer.id
  };

  if (!isPostgresEnabled()) {
    const sqliteDb = getSqlite();
    sqliteDb.prepare(
      `INSERT INTO users (id, phone, password_hash, mpin_hash, name, joined_at, referral_code, role, approval_status, approved_at, rejected_at, signup_bonus_granted, referred_by_user_id)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
    ).run(
      user.id,
      user.phone,
      user.passwordHash,
      user.mpinHash,
      user.name,
      user.joinedAt,
      user.referralCode,
      user.role,
      user.approvalStatus,
      user.approvedAt,
      user.rejectedAt,
      user.signupBonusGranted ? 1 : 0,
      user.referredByUserId
    );
    return { user, error: null } as const;
  }

  await ensurePostgresReady();
  await getPgPool().query(
    `INSERT INTO users (id, phone, password_hash, mpin_hash, name, joined_at, referral_code, role, approval_status, approved_at, rejected_at, signup_bonus_granted, referred_by_user_id)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)`,
    [
      user.id,
      user.phone,
      user.passwordHash,
      user.mpinHash,
      user.name,
      user.joinedAt,
      user.referralCode,
      user.role,
      user.approvalStatus,
      user.approvedAt,
      user.rejectedAt,
      user.signupBonusGranted,
      user.referredByUserId
    ]
  );

  return { user, error: null } as const;
}

export async function findUserByPhone(phone: string) {
  const normalizedPhone = phone.trim();

  if (!isPostgresEnabled()) {
    const db = getSqlite();
    const row = db.prepare("SELECT * FROM users WHERE phone = ? LIMIT 1").get(normalizedPhone) as Record<string, unknown> | undefined;
    return row ? mapUser(row as never) : null;
  }

  await ensurePostgresReady();
  const result = await getPgPool().query("SELECT * FROM users WHERE phone = $1 LIMIT 1", [normalizedPhone]);
  return result.rows[0] ? mapUser(result.rows[0] as never) : null;
}

async function hasReferralCode(referralCode: string) {
  const normalizedReferralCode = referralCode.trim();

  if (!isPostgresEnabled()) {
    const db = getSqlite();
    const row = db.prepare("SELECT id FROM users WHERE referral_code = ? LIMIT 1").get(normalizedReferralCode) as { id: string } | undefined;
    return Boolean(row);
  }

  await ensurePostgresReady();
  const result = await getPgPool().query("SELECT id FROM users WHERE referral_code = $1 LIMIT 1", [normalizedReferralCode]);
  return Boolean(result.rows[0]);
}

export async function findUserByReferralCode(referralCode: string) {
  const normalizedReferralCode = referralCode.trim();

  if (!isPostgresEnabled()) {
    const db = getSqlite();
    const row = db.prepare("SELECT * FROM users WHERE referral_code = ? LIMIT 1").get(normalizedReferralCode) as Record<string, unknown> | undefined;
    return row ? mapUser(row as never) : null;
  }

  await ensurePostgresReady();
  const result = await getPgPool().query("SELECT * FROM users WHERE referral_code = $1 LIMIT 1", [normalizedReferralCode]);
  return result.rows[0] ? mapUser(result.rows[0] as never) : null;
}

export async function createOtpChallenge(input: { phone: string; purpose: OtpChallenge["purpose"]; codeHash: string; expiresAt: string }) {
  const challenge: OtpChallenge = {
    id: `otp_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
    phone: input.phone.trim(),
    codeHash: input.codeHash,
    purpose: input.purpose,
    expiresAt: input.expiresAt,
    consumedAt: null,
    createdAt: now()
  };

  if (!isPostgresEnabled()) {
    const db = getSqlite();
    db.prepare("DELETE FROM otp_challenges WHERE phone = ? AND purpose = ?").run(challenge.phone, challenge.purpose);
    db.prepare(
      `INSERT INTO otp_challenges (id, phone, code_hash, purpose, expires_at, consumed_at, created_at)
       VALUES (?, ?, ?, ?, ?, ?, ?)`
    ).run(challenge.id, challenge.phone, challenge.codeHash, challenge.purpose, challenge.expiresAt, null, challenge.createdAt);
    return challenge;
  }

  await ensurePostgresReady();
  await getPgPool().query("DELETE FROM otp_challenges WHERE phone = $1 AND purpose = $2", [challenge.phone, challenge.purpose]);
  await getPgPool().query(
    `INSERT INTO otp_challenges (id, phone, code_hash, purpose, expires_at, consumed_at, created_at)
     VALUES ($1, $2, $3, $4, $5, $6, $7)`,
    [challenge.id, challenge.phone, challenge.codeHash, challenge.purpose, challenge.expiresAt, null, challenge.createdAt]
  );
  return challenge;
}

export async function consumeOtpChallenge(phone: string, purpose: OtpChallenge["purpose"], code: string) {
  const normalizedPhone = phone.trim();

  if (!isPostgresEnabled()) {
    const db = getSqlite();
    const row = db.prepare(
      `SELECT * FROM otp_challenges
       WHERE phone = ? AND purpose = ? AND consumed_at IS NULL
       ORDER BY created_at DESC LIMIT 1`
    ).get(normalizedPhone, purpose) as Record<string, unknown> | undefined;

    if (!row) {
      return null;
    }

    const challenge = mapOtpChallenge(row as never);
    if (new Date(challenge.expiresAt).getTime() < Date.now() || !verifyCredential(code.trim(), challenge.codeHash)) {
      return null;
    }

    const consumedAt = now();
    db.prepare("UPDATE otp_challenges SET consumed_at = ? WHERE id = ?").run(consumedAt, challenge.id);
    return { ...challenge, consumedAt };
  }

  await ensurePostgresReady();
  const result = await getPgPool().query(
    `SELECT * FROM otp_challenges
     WHERE phone = $1 AND purpose = $2 AND consumed_at IS NULL
     ORDER BY created_at DESC LIMIT 1`,
    [normalizedPhone, purpose]
  );

  if (!result.rows[0]) {
    return null;
  }

  const challenge = mapOtpChallenge(result.rows[0] as never);
  if (new Date(challenge.expiresAt).getTime() < Date.now() || !verifyCredential(code.trim(), challenge.codeHash)) {
    return null;
  }

  const consumedAt = now();
  await getPgPool().query("UPDATE otp_challenges SET consumed_at = $1 WHERE id = $2", [consumedAt, challenge.id]);
  return { ...challenge, consumedAt };
}

export async function revokeSession(token?: string | null) {
  if (!token) {
    return;
  }

  const tokenHash = hashSecret(token);
  if (!isPostgresEnabled()) {
    const db = getSqlite();
    db.prepare("DELETE FROM sessions WHERE token_hash = ?").run(tokenHash);
    return;
  }

  await ensurePostgresReady();
  await getPgPool().query("DELETE FROM sessions WHERE token_hash = $1", [tokenHash]);
}

export async function findUserByToken(token?: string | null) {
  if (!token) {
    return null;
  }

  const tokenHash = hashSecret(token);
  if (!isPostgresEnabled()) {
    const db = getSqlite();
    const sessionRow = db.prepare("SELECT * FROM sessions WHERE token_hash = ? LIMIT 1").get(tokenHash) as
      | { token_hash: string; user_id: string; created_at: string }
      | undefined;

    if (!sessionRow) {
      return null;
    }

    if (new Date(sessionRow.created_at).getTime() + sessionTtlMs < Date.now()) {
      db.prepare("DELETE FROM sessions WHERE token_hash = ?").run(tokenHash);
      return null;
    }

    const userRow = db.prepare("SELECT * FROM users WHERE id = ? LIMIT 1").get(sessionRow.user_id) as
      | {
          id: string;
          phone: string;
          password_hash: string;
          mpin_hash: string;
          name: string;
          joined_at: string;
          referral_code: string;
          role?: "admin" | "user";
        }
      | undefined;

    return userRow ? mapUser(userRow) : null;
  }

  await ensurePostgresReady();
  const result = await getPgPool().query(
    `SELECT u.*, s.created_at AS session_created_at
     FROM sessions s
     JOIN users u ON u.id = s.user_id
     WHERE s.token_hash = $1
     LIMIT 1`,
    [tokenHash]
  );
  if (!result.rows[0]) {
    return null;
  }
  const sessionCreatedAt = String((result.rows[0] as { session_created_at?: string }).session_created_at ?? "");
  if (sessionCreatedAt && new Date(sessionCreatedAt).getTime() + sessionTtlMs < Date.now()) {
    await getPgPool().query("DELETE FROM sessions WHERE token_hash = $1", [tokenHash]);
    return null;
  }
  return mapUser(result.rows[0] as never);
}

export async function getUserBalance(userId: string) {
  if (!isPostgresEnabled()) {
    const db = getSqlite();
    const row = db.prepare("SELECT after_balance FROM wallet_entries WHERE user_id = ? ORDER BY created_at DESC, id DESC LIMIT 1").get(userId) as
      | { after_balance: number }
      | undefined;
    return row ? Number(row.after_balance) : 0;
  }

  await ensurePostgresReady();
  const result = await getPgPool().query(
    "SELECT after_balance FROM wallet_entries WHERE user_id = $1 ORDER BY created_at DESC, id DESC LIMIT 1",
    [userId]
  );
  return result.rows[0] ? Number(result.rows[0].after_balance) : 0;
}

export async function addWalletEntry(entry: Omit<WalletLedgerEntry, "id" | "createdAt">) {
  const next: WalletLedgerEntry = {
    ...entry,
    id: `wallet_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
    createdAt: now()
  };

  if (!isPostgresEnabled()) {
    const db = getSqlite();
    db.prepare(
      `INSERT INTO wallet_entries (id, user_id, type, status, amount, before_balance, after_balance, created_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
    ).run(next.id, next.userId, next.type, next.status, next.amount, next.beforeBalance, next.afterBalance, next.createdAt);
    return next;
  }

  await ensurePostgresReady();
  await getPgPool().query(
    `INSERT INTO wallet_entries (id, user_id, type, status, amount, before_balance, after_balance, created_at)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
    [next.id, next.userId, next.type, next.status, next.amount, next.beforeBalance, next.afterBalance, next.createdAt]
  );
  return next;
}

export async function addBid(entry: Omit<Bid, "id" | "createdAt">) {
  const next: Bid = {
    ...entry,
    id: `bid_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
    createdAt: now()
  };

  if (!isPostgresEnabled()) {
    const db = getSqlite();
    db.prepare(
      `INSERT INTO bids (id, user_id, market, board_label, session_type, digit, points, status, payout, settled_at, settled_result, created_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
    ).run(
      next.id,
      next.userId,
      next.market,
      next.boardLabel,
      next.sessionType,
      next.digit,
      next.points,
      next.status,
      next.payout,
      next.settledAt,
      next.settledResult,
      next.createdAt
    );
    return next;
  }

  await ensurePostgresReady();
  await getPgPool().query(
    `INSERT INTO bids (id, user_id, market, board_label, session_type, digit, points, status, payout, settled_at, settled_result, created_at)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
    [
      next.id,
      next.userId,
      next.market,
      next.boardLabel,
      next.sessionType,
      next.digit,
      next.points,
      next.status,
      next.payout,
      next.settledAt,
      next.settledResult,
      next.createdAt
    ]
  );
  return next;
}

export async function addBankAccount(entry: Omit<BankAccount, "id" | "createdAt">) {
  const next: BankAccount = {
    ...entry,
    id: `bank_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
    createdAt: now()
  };

  if (!isPostgresEnabled()) {
    const db = getSqlite();
    db.prepare(
      `INSERT INTO bank_accounts (id, user_id, account_number, holder_name, ifsc, created_at)
       VALUES (?, ?, ?, ?, ?, ?)`
    ).run(next.id, next.userId, next.accountNumber, next.holderName, next.ifsc, next.createdAt);
    return next;
  }

  await ensurePostgresReady();
  await getPgPool().query(
    `INSERT INTO bank_accounts (id, user_id, account_number, holder_name, ifsc, created_at)
     VALUES ($1, $2, $3, $4, $5, $6)`,
    [next.id, next.userId, next.accountNumber, next.holderName, next.ifsc, next.createdAt]
  );
  return next;
}

export async function updateUserProfile(userId: string, updates: { name?: string; phone?: string }) {
  const current = await findUserById(userId);
  if (!current) {
    return null;
  }

  const nextName = updates.name?.trim() || current.name;
  const nextPhone = updates.phone?.trim() || current.phone;

  if (!isPostgresEnabled()) {
    const db = getSqlite();
    db.prepare("UPDATE users SET name = ?, phone = ? WHERE id = ?").run(nextName, nextPhone, userId);
    return findUserById(userId);
  }

  await ensurePostgresReady();
  await getPgPool().query("UPDATE users SET name = $1, phone = $2 WHERE id = $3", [nextName, nextPhone, userId]);
  return findUserById(userId);
}

export async function updateUserPassword(userId: string, passwordHash: string) {
  if (!isPostgresEnabled()) {
    const db = getSqlite();
    db.prepare("UPDATE users SET password_hash = ? WHERE id = ?").run(passwordHash, userId);
    return findUserById(userId);
  }

  await ensurePostgresReady();
  await getPgPool().query("UPDATE users SET password_hash = $1 WHERE id = $2", [passwordHash, userId]);
  return findUserById(userId);
}

export async function updateUserMpin(userId: string, mpinHash: string) {
  if (!isPostgresEnabled()) {
    const db = getSqlite();
    db.prepare("UPDATE users SET mpin_hash = ? WHERE id = ?").run(mpinHash, userId);
    return findUserById(userId);
  }

  await ensurePostgresReady();
  await getPgPool().query("UPDATE users SET mpin_hash = $1 WHERE id = $2", [mpinHash, userId]);
  return findUserById(userId);
}

export async function updateMarketRecord(
  slug: string,
  updates: { result?: string; status?: string; action?: string; open?: string; close?: string; category?: Market["category"] }
) {
  const current = await findMarketBySlug(slug);
  if (!current) {
    return null;
  }

  const nextResult = updates.result?.trim() || current.result;
  const nextStatus = updates.status?.trim() || current.status;
  const nextAction = updates.action?.trim() || current.action;
  const nextOpen = updates.open?.trim() || current.open;
  const nextClose = updates.close?.trim() || current.close;
  const nextCategory = updates.category || current.category;

  if (!isPostgresEnabled()) {
    const db = getSqlite();
    db.prepare(
      `UPDATE markets
       SET result = ?, status = ?, action = ?, open_time = ?, close_time = ?, category = ?
       WHERE slug = ?`
    ).run(nextResult, nextStatus, nextAction, nextOpen, nextClose, nextCategory, slug);
    return findMarketBySlug(slug);
  }

  await ensurePostgresReady();
  await getPgPool().query(
    `UPDATE markets
     SET result = $1, status = $2, action = $3, open_time = $4, close_time = $5, category = $6
     WHERE slug = $7`,
    [nextResult, nextStatus, nextAction, nextOpen, nextClose, nextCategory, slug]
  );
  return findMarketBySlug(slug);
}

export async function upsertChartRecord(marketSlug: string, chartType: ChartRecord["chartType"], rows: string[][]) {
  if (!isPostgresEnabled()) {
    const db = getSqlite();
    db.prepare(
      `INSERT INTO charts (market_slug, chart_type, rows_json)
       VALUES (?, ?, ?)
       ON CONFLICT(market_slug, chart_type) DO UPDATE SET rows_json = excluded.rows_json`
    ).run(marketSlug, chartType, JSON.stringify(rows));
    const updated = db.prepare("SELECT * FROM charts WHERE market_slug = ? AND chart_type = ? LIMIT 1").get(marketSlug, chartType) as
      | { market_slug: string; chart_type: ChartRecord["chartType"]; rows_json: string }
      | undefined;
    return updated ? mapChart(updated) : null;
  }

  await ensurePostgresReady();
  const result = await getPgPool().query(
    `INSERT INTO charts (market_slug, chart_type, rows_json)
     VALUES ($1, $2, $3::jsonb)
     ON CONFLICT (market_slug, chart_type) DO UPDATE SET rows_json = EXCLUDED.rows_json
     RETURNING market_slug, chart_type, rows_json::text AS rows_json`,
    [marketSlug, chartType, JSON.stringify(rows)]
  );
  return result.rows[0] ? mapChart(result.rows[0] as never) : null;
}

export async function findUserById(userId: string) {
  if (!isPostgresEnabled()) {
    const db = getSqlite();
    const row = db.prepare("SELECT * FROM users WHERE id = ? LIMIT 1").get(userId) as
      | {
          id: string;
          phone: string;
          password_hash: string;
          mpin_hash: string;
          name: string;
          joined_at: string;
          referral_code: string;
          role?: "admin" | "user";
        }
      | undefined;
    return row ? mapUser(row) : null;
  }

  await ensurePostgresReady();
  const result = await getPgPool().query("SELECT * FROM users WHERE id = $1 LIMIT 1", [userId]);
  return result.rows[0] ? mapUser(result.rows[0] as never) : null;
}

export async function getUsersList() {
  if (!isPostgresEnabled()) {
    const db = getSqlite();
    return (db.prepare("SELECT * FROM users ORDER BY joined_at DESC, id DESC").all() as Array<Record<string, unknown>>).map((row) => mapUser(row as never));
  }

  await ensurePostgresReady();
  const result = await getPgPool().query("SELECT * FROM users ORDER BY joined_at DESC, id DESC");
  return result.rows.map((row: Record<string, unknown>) => mapUser(row as never));
}

export async function updateUserApprovalStatus(userId: string, status: User["approvalStatus"]) {
  const current = await findUserById(userId);
  if (!current) {
    return null;
  }

  const approvedAt = status === "Approved" ? now() : null;
  const rejectedAt = status === "Rejected" ? now() : null;
  const signupBonusGranted = status === "Approved" ? current.signupBonusGranted || true : current.signupBonusGranted;

  if (!isPostgresEnabled()) {
    const db = getSqlite();
    db.prepare(
      "UPDATE users SET approval_status = ?, approved_at = ?, rejected_at = ?, signup_bonus_granted = ? WHERE id = ?"
    ).run(status, approvedAt, rejectedAt, signupBonusGranted ? 1 : 0, userId);
  } else {
    await ensurePostgresReady();
    await getPgPool().query(
      "UPDATE users SET approval_status = $1, approved_at = $2, rejected_at = $3, signup_bonus_granted = $4 WHERE id = $5",
      [status, approvedAt, rejectedAt, signupBonusGranted, userId]
    );
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

export async function findMarketBySlug(slug: string) {
  if (!isPostgresEnabled()) {
    const db = getSqlite();
    const row = db.prepare("SELECT * FROM markets WHERE slug = ? LIMIT 1").get(slug) as Record<string, unknown> | undefined;
    return row ? mapMarket(row as never) : null;
  }

  await ensurePostgresReady();
  const result = await getPgPool().query("SELECT * FROM markets WHERE slug = $1 LIMIT 1", [slug]);
  return result.rows[0] ? mapMarket(result.rows[0] as never) : null;
}

export async function getBidsForMarket(marketName: string) {
  if (!isPostgresEnabled()) {
    const db = getSqlite();
    return (db.prepare("SELECT * FROM bids WHERE market = ? ORDER BY created_at ASC, id ASC").all(marketName) as Array<Record<string, unknown>>).map((row) =>
      mapBid(row as never)
    );
  }

  await ensurePostgresReady();
  const result = await getPgPool().query("SELECT * FROM bids WHERE market = $1 ORDER BY created_at ASC, id ASC", [marketName]);
  return result.rows.map((row: Record<string, unknown>) => mapBid(row as never));
}

export async function getBidsForUser(userId: string) {
  if (!isPostgresEnabled()) {
    const db = getSqlite();
    return (db.prepare("SELECT * FROM bids WHERE user_id = ? ORDER BY created_at DESC, id DESC").all(userId) as Array<Record<string, unknown>>).map((row) =>
      mapBid(row as never)
    );
  }

  await ensurePostgresReady();
  const result = await getPgPool().query("SELECT * FROM bids WHERE user_id = $1 ORDER BY created_at DESC, id DESC", [userId]);
  return result.rows.map((row: Record<string, unknown>) => mapBid(row as never));
}

export async function getWalletEntriesForUser(userId: string) {
  if (!isPostgresEnabled()) {
    const db = getSqlite();
    return (
      db.prepare("SELECT * FROM wallet_entries WHERE user_id = ? ORDER BY created_at DESC, id DESC").all(userId) as Array<Record<string, unknown>>
    ).map((row) => mapWalletEntry(row as never));
  }

  await ensurePostgresReady();
  const result = await getPgPool().query("SELECT * FROM wallet_entries WHERE user_id = $1 ORDER BY created_at DESC, id DESC", [userId]);
  return result.rows.map((row: Record<string, unknown>) => mapWalletEntry(row as never));
}

export async function getWalletApprovalRequests() {
  const filters = ["DEPOSIT", "WITHDRAW"];

  if (!isPostgresEnabled()) {
    const db = getSqlite();
    return (
      db
        .prepare("SELECT * FROM wallet_entries WHERE status = ? AND type IN (?, ?) ORDER BY created_at DESC, id DESC")
        .all("INITIATED", filters[0], filters[1]) as Array<Record<string, unknown>>
    ).map((row) => mapWalletEntry(row as never));
  }

  await ensurePostgresReady();
  const result = await getPgPool().query(
    "SELECT * FROM wallet_entries WHERE status = $1 AND type = ANY($2::text[]) ORDER BY created_at DESC, id DESC",
    ["INITIATED", filters]
  );
  return result.rows.map((row: Record<string, unknown>) => mapWalletEntry(row as never));
}

async function findWalletEntryById(entryId: string) {
  if (!isPostgresEnabled()) {
    const db = getSqlite();
    const row = db.prepare("SELECT * FROM wallet_entries WHERE id = ? LIMIT 1").get(entryId) as Record<string, unknown> | undefined;
    return row ? mapWalletEntry(row as never) : null;
  }

  await ensurePostgresReady();
  const result = await getPgPool().query("SELECT * FROM wallet_entries WHERE id = $1 LIMIT 1", [entryId]);
  return result.rows[0] ? mapWalletEntry(result.rows[0] as never) : null;
}

async function updateWalletEntryStatus(entryId: string, status: WalletLedgerEntry["status"]) {
  if (!isPostgresEnabled()) {
    const db = getSqlite();
    db.prepare("UPDATE wallet_entries SET status = ? WHERE id = ?").run(status, entryId);
    return findWalletEntryById(entryId);
  }

  await ensurePostgresReady();
  await getPgPool().query("UPDATE wallet_entries SET status = $1 WHERE id = $2", [status, entryId]);
  return findWalletEntryById(entryId);
}

export async function resolveWalletApprovalRequest(entryId: string, action: "approve" | "reject") {
  const request = await findWalletEntryById(entryId);
  if (!request || request.status !== "INITIATED" || !["DEPOSIT", "WITHDRAW"].includes(request.type)) {
    return null;
  }

  if (action === "reject") {
    const updatedRequest = await updateWalletEntryStatus(entryId, "REJECTED");
    return {
      request: updatedRequest,
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
  const updatedRequest = await updateWalletEntryStatus(entryId, "BACKOFFICE");

  return {
    request: updatedRequest,
    settlementEntry
  };
}

export async function getBankAccountsForUser(userId: string) {
  if (!isPostgresEnabled()) {
    const db = getSqlite();
    return (
      db.prepare("SELECT * FROM bank_accounts WHERE user_id = ? ORDER BY created_at DESC, id DESC").all(userId) as Array<Record<string, unknown>>
    ).map((row) => mapBankAccount(row as never));
  }

  await ensurePostgresReady();
  const result = await getPgPool().query("SELECT * FROM bank_accounts WHERE user_id = $1 ORDER BY created_at DESC, id DESC", [userId]);
  return result.rows.map((row: Record<string, unknown>) => mapBankAccount(row as never));
}

export async function updateBidSettlement(bidId: string, status: Bid["status"], payout: number, settledResult: string) {
  const settledAt = status === "Pending" ? null : now();
  const normalizedResult = status === "Pending" ? null : settledResult;

  if (!isPostgresEnabled()) {
    const db = getSqlite();
    db.prepare("UPDATE bids SET status = ?, payout = ?, settled_at = ?, settled_result = ? WHERE id = ?").run(
      status,
      payout,
      settledAt,
      normalizedResult,
      bidId
    );
    const row = db.prepare("SELECT * FROM bids WHERE id = ? LIMIT 1").get(bidId) as Record<string, unknown> | undefined;
    return row ? mapBid(row as never) : null;
  }

  await ensurePostgresReady();
  const result = await getPgPool().query(
    `UPDATE bids
     SET status = $1, payout = $2, settled_at = $3, settled_result = $4
     WHERE id = $5
     RETURNING *`,
    [status, payout, settledAt, normalizedResult, bidId]
  );
  return result.rows[0] ? mapBid(result.rows[0] as never) : null;
}

export async function requireUser(token?: string | null) {
  return findUserByToken(token);
}

export async function addAuditLog(entry: { actorUserId: string; action: string; entityType: string; entityId: string; details: string }) {
  const id = `audit_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
  const createdAt = now();

  if (!isPostgresEnabled()) {
    const db = getSqlite();
    db.prepare(
      `INSERT INTO audit_logs (id, actor_user_id, action, entity_type, entity_id, details, created_at)
       VALUES (?, ?, ?, ?, ?, ?, ?)`
    ).run(id, entry.actorUserId, entry.action, entry.entityType, entry.entityId, entry.details, createdAt);
    return { id, createdAt, ...entry };
  }

  await ensurePostgresReady();
  await getPgPool().query(
    `INSERT INTO audit_logs (id, actor_user_id, action, entity_type, entity_id, details, created_at)
     VALUES ($1, $2, $3, $4, $5, $6, $7)`,
    [id, entry.actorUserId, entry.action, entry.entityType, entry.entityId, entry.details, createdAt]
  );
  return { id, createdAt, ...entry };
}

export async function upsertNotificationDevice(entry: {
  userId: string;
  platform: "android" | "ios" | "web";
  token: string;
  enabled: boolean;
}) {
  const updatedAt = now();

  if (!isPostgresEnabled()) {
    const db = getSqlite();
    const existing = db.prepare("SELECT id FROM notification_devices WHERE user_id = ? AND token = ? LIMIT 1").get(entry.userId, entry.token) as
      | { id: string }
      | undefined;

    if (existing) {
      db.prepare("UPDATE notification_devices SET platform = ?, enabled = ?, updated_at = ? WHERE id = ?").run(
        entry.platform,
        entry.enabled ? 1 : 0,
        updatedAt,
        existing.id
      );
      const row = db.prepare("SELECT * FROM notification_devices WHERE id = ? LIMIT 1").get(existing.id) as Record<string, unknown>;
      return mapNotificationDevice(row as never);
    }

    const id = `device_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
    db.prepare(
      `INSERT INTO notification_devices (id, user_id, platform, token, enabled, created_at, updated_at)
       VALUES (?, ?, ?, ?, ?, ?, ?)`
    ).run(id, entry.userId, entry.platform, entry.token, entry.enabled ? 1 : 0, updatedAt, updatedAt);
    return mapNotificationDevice(db.prepare("SELECT * FROM notification_devices WHERE id = ? LIMIT 1").get(id) as never);
  }

  await ensurePostgresReady();
  const id = `device_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
  const result = await getPgPool().query(
    `INSERT INTO notification_devices (id, user_id, platform, token, enabled, created_at, updated_at)
     VALUES ($1, $2, $3, $4, $5, $6, $7)
     ON CONFLICT (user_id, token) DO UPDATE SET
       platform = EXCLUDED.platform,
       enabled = EXCLUDED.enabled,
       updated_at = EXCLUDED.updated_at
     RETURNING *`,
    [id, entry.userId, entry.platform, entry.token, entry.enabled, updatedAt, updatedAt]
  );
  return mapNotificationDevice(result.rows[0] as never);
}

export async function addNotificationRecord(entry: Omit<NotificationRecord, "id" | "createdAt">) {
  const next: NotificationRecord = {
    ...entry,
    id: `notification_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
    createdAt: now()
  };

  if (!isPostgresEnabled()) {
    const db = getSqlite();
    db.prepare(
      `INSERT INTO notifications (id, user_id, title, body, channel, read, created_at)
       VALUES (?, ?, ?, ?, ?, ?, ?)`
    ).run(next.id, next.userId, next.title, next.body, next.channel, next.read ? 1 : 0, next.createdAt);
    return next;
  }

  await ensurePostgresReady();
  await getPgPool().query(
    `INSERT INTO notifications (id, user_id, title, body, channel, read, created_at)
     VALUES ($1, $2, $3, $4, $5, $6, $7)`,
    [next.id, next.userId, next.title, next.body, next.channel, next.read, next.createdAt]
  );
  return next;
}

export async function addPaymentOrder(entry: Omit<PaymentOrder, "id" | "createdAt" | "updatedAt">) {
  const timestamp = now();
  const next: PaymentOrder = {
    ...entry,
    id: `payment_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
    createdAt: timestamp,
    updatedAt: timestamp
  };

  if (!isPostgresEnabled()) {
    const db = getSqlite();
    db.prepare(
      `INSERT INTO payment_orders (id, user_id, provider, amount, status, reference, redirect_url, created_at, updated_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
    ).run(next.id, next.userId, next.provider, next.amount, next.status, next.reference, next.redirectUrl, next.createdAt, next.updatedAt);
    return next;
  }

  await ensurePostgresReady();
  await getPgPool().query(
    `INSERT INTO payment_orders (id, user_id, provider, amount, status, reference, redirect_url, created_at, updated_at)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
    [next.id, next.userId, next.provider, next.amount, next.status, next.reference, next.redirectUrl, next.createdAt, next.updatedAt]
  );
  return next;
}

export async function updatePaymentOrderStatus(reference: string, status: PaymentOrder["status"]) {
  const updatedAt = now();

  if (!isPostgresEnabled()) {
    const db = getSqlite();
    db.prepare("UPDATE payment_orders SET status = ?, updated_at = ? WHERE reference = ?").run(status, updatedAt, reference);
    const row = db.prepare("SELECT * FROM payment_orders WHERE reference = ? LIMIT 1").get(reference) as Record<string, unknown> | undefined;
    return row ? mapPaymentOrder(row as never) : null;
  }

  await ensurePostgresReady();
  const result = await getPgPool().query(
    `UPDATE payment_orders SET status = $1, updated_at = $2 WHERE reference = $3 RETURNING *`,
    [status, updatedAt, reference]
  );
  return result.rows[0] ? mapPaymentOrder(result.rows[0] as never) : null;
}

