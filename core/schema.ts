export type User = {
  id: string;
  phone: string;
  passwordHash: string;
  mpinHash: string;
  name: string;
  joinedAt: string;
  referralCode: string;
  role: "admin" | "user";
  approvalStatus: "Pending" | "Approved" | "Rejected";
  approvedAt: string | null;
  rejectedAt: string | null;
  signupBonusGranted: boolean;
  referredByUserId: string | null;
};

export type Session = {
  tokenHash: string;
  userId: string;
  createdAt: string;
};

export type OtpChallenge = {
  id: string;
  phone: string;
  codeHash: string;
  purpose: "login" | "password_reset";
  expiresAt: string;
  consumedAt: string | null;
  createdAt: string;
};

export type WalletLedgerEntry = {
  id: string;
  userId: string;
  type: "DEPOSIT" | "WITHDRAW" | "BID_PLACED" | "BID_WIN" | "REFERRAL_COMMISSION" | "SIGNUP_BONUS";
  status: "INITIATED" | "SUCCESS" | "BACKOFFICE" | "REJECTED";
  amount: number;
  beforeBalance: number;
  afterBalance: number;
  createdAt: string;
};

export type Bid = {
  id: string;
  userId: string;
  market: string;
  boardLabel: string;
  sessionType: "Open" | "Close";
  digit: string;
  points: number;
  status: "Pending" | "Won" | "Lost";
  payout: number;
  settledAt: string | null;
  settledResult: string | null;
  createdAt: string;
};

export type BankAccount = {
  id: string;
  userId: string;
  accountNumber: string;
  holderName: string;
  ifsc: string;
  createdAt: string;
};

export type Market = {
  id: string;
  slug: string;
  name: string;
  result: string;
  status: string;
  action: string;
  open: string;
  close: string;
  category: "starline" | "games" | "jackpot";
};

export type ChartRecord = {
  marketSlug: string;
  chartType: "jodi" | "panna";
  rows: string[][];
};

export type NotificationDevice = {
  id: string;
  userId: string;
  platform: "android" | "ios" | "web";
  token: string;
  enabled: boolean;
  createdAt: string;
  updatedAt: string;
};

export type NotificationRecord = {
  id: string;
  userId: string;
  title: string;
  body: string;
  channel: "system" | "wallet" | "market" | "security" | "promotion";
  read: boolean;
  createdAt: string;
};

export type PaymentOrder = {
  id: string;
  userId: string;
  provider: "manual" | "razorpay" | "cashfree" | "phonepe";
  amount: number;
  status: "CREATED" | "PENDING" | "SUCCESS" | "FAILED";
  reference: string;
  redirectUrl: string | null;
  createdAt: string;
  updatedAt: string;
};

export type Database = {
  users: User[];
  sessions: Session[];
  walletEntries: WalletLedgerEntry[];
  bids: Bid[];
  bankAccounts: BankAccount[];
  markets: Market[];
  charts: ChartRecord[];
  otpChallenges: OtpChallenge[];
  notificationDevices: NotificationDevice[];
  notifications: NotificationRecord[];
  paymentOrders: PaymentOrder[];
  auditLogs: AuditLog[];
};

export type AuditLog = {
  id: string;
  actorUserId: string;
  action: string;
  entityType: string;
  entityId: string;
  details: string;
  createdAt: string;
};
