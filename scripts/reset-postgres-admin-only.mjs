import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { Pool } from "pg";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const backendRoot = path.resolve(__dirname, "..");
const envPath = path.join(backendRoot, ".env.local");

for (const line of fs.readFileSync(envPath, "utf8").split(/\r?\n/)) {
  const trimmed = line.trim();
  if (!trimmed || trimmed.startsWith("#")) continue;
  const index = trimmed.indexOf("=");
  if (index <= 0) continue;
  const key = trimmed.slice(0, index).trim();
  const value = trimmed.slice(index + 1).trim().replace(/^['"]|['"]$/g, "");
  process.env[key] = value;
}

const adminUserId = "user_1";
const defaultReferralCode = "621356";
const adminJoinedAt = "2025-04-12T10:00:00.000Z";

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: false
});

const client = await pool.connect();

try {
  await client.query("BEGIN");

  await client.query("DELETE FROM sessions");
  await client.query("DELETE FROM notification_devices");
  await client.query("DELETE FROM notifications WHERE user_id <> $1", [adminUserId]);
  await client.query("DELETE FROM bank_accounts WHERE user_id <> $1", [adminUserId]);
  await client.query("DELETE FROM bids WHERE user_id <> $1", [adminUserId]);
  await client.query("DELETE FROM wallet_entries WHERE user_id <> $1", [adminUserId]);
  await client.query("DELETE FROM payment_orders WHERE user_id <> $1", [adminUserId]);
  await client.query("DELETE FROM audit_logs WHERE actor_user_id <> $1", [adminUserId]);
  await client.query("DELETE FROM users WHERE id <> $1", [adminUserId]);

  await client.query(
    `UPDATE users
     SET role = 'admin',
         approval_status = 'Approved',
         approved_at = $2,
         rejected_at = NULL,
         signup_bonus_granted = TRUE,
         referral_code = $3
     WHERE id = $1`,
    [adminUserId, adminJoinedAt, defaultReferralCode]
  );

  await client.query("DELETE FROM wallet_entries WHERE user_id = $1", [adminUserId]);
  await client.query(
    `INSERT INTO wallet_entries (id, user_id, type, status, amount, before_balance, after_balance, created_at)
     VALUES ('wallet_1', $1, 'DEPOSIT', 'SUCCESS', 0, 0, 0, $2)`,
    [adminUserId, new Date().toISOString()]
  );

  await client.query("COMMIT");
  console.log("Postgres reset complete: admin-only fresh state.");
} catch (error) {
  await client.query("ROLLBACK");
  throw error;
} finally {
  client.release();
  await pool.end();
}
