CREATE TABLE users (
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

CREATE TABLE sessions (
  token_hash TEXT PRIMARY KEY,
  user_id TEXT NOT NULL REFERENCES users(id),
  created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE otp_challenges (
  id TEXT PRIMARY KEY,
  phone TEXT NOT NULL,
  code_hash TEXT NOT NULL,
  purpose TEXT NOT NULL,
  expires_at TIMESTAMPTZ NOT NULL,
  consumed_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE wallet_entries (
  id TEXT PRIMARY KEY,
  user_id TEXT NOT NULL REFERENCES users(id),
  type TEXT NOT NULL,
  status TEXT NOT NULL,
  amount NUMERIC(12,2) NOT NULL,
  before_balance NUMERIC(12,2) NOT NULL,
  after_balance NUMERIC(12,2) NOT NULL,
  created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE bids (
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

CREATE TABLE bank_accounts (
  id TEXT PRIMARY KEY,
  user_id TEXT NOT NULL REFERENCES users(id),
  account_number TEXT NOT NULL,
  holder_name TEXT NOT NULL,
  ifsc TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE markets (
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

CREATE TABLE charts (
  market_slug TEXT NOT NULL REFERENCES markets(slug),
  chart_type TEXT NOT NULL,
  rows_json JSONB NOT NULL,
  PRIMARY KEY (market_slug, chart_type)
);

CREATE TABLE audit_logs (
  id TEXT PRIMARY KEY,
  actor_user_id TEXT NOT NULL REFERENCES users(id),
  action TEXT NOT NULL,
  entity_type TEXT NOT NULL,
  entity_id TEXT NOT NULL,
  details TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE notification_devices (
  id TEXT PRIMARY KEY,
  user_id TEXT NOT NULL REFERENCES users(id),
  platform TEXT NOT NULL,
  token TEXT NOT NULL,
  enabled BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL,
  UNIQUE (user_id, token)
);

CREATE TABLE notifications (
  id TEXT PRIMARY KEY,
  user_id TEXT NOT NULL REFERENCES users(id),
  title TEXT NOT NULL,
  body TEXT NOT NULL,
  channel TEXT NOT NULL,
  read BOOLEAN NOT NULL DEFAULT FALSE,
  created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE payment_orders (
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
