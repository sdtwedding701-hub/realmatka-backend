import {
  addAuditLog,
  addWalletEntry,
  createNotification,
  findMarketBySlug,
  findUserById,
  getAppSettings,
  getAdminSnapshot,
  getAuditLogs,
  getBankAccountsForUser,
  listAllBids,
  listAllNotifications,
  getBidsForMarket,
  getBidsForUser,
  getChartRecord,
  getUserBalance,
  getUsersList,
  getWalletApprovalRequests,
  getWalletEntriesForUser,
  getWalletRequestHistory,
  requireUserByToken,
  resolveWalletApprovalRequest,
  updateWalletEntryAdmin,
  updateUserAccountStatus,
  updateBidSettlement,
  updateMarketRecord,
  updateUserApprovalStatus,
  upsertAppSetting,
  upsertChartRecord
} from "../db.mjs";
import { getPannaType, getSattaCardDigit, isValidPanna } from "../matka-rules.mjs";
import { corsPreflight, fail, getJsonBody, getSessionToken, ok, unauthorized } from "../http.mjs";

const payoutRates = {
  "Single Digit": 10,
  "Single Digit Bulk": 10,
  "Jodi Digit": 100,
  "Jodi Digit Bulk": 100,
  "Group Jodi": 100,
  "Red Bracket": 100,
  "Digit Based Jodi": 100,
  "Two Digit Panel (CP,SR)": 100,
  "Single Pana": 160,
  "Single Pana Bulk": 160,
  "Choice Pana": 160,
  "SP Motor": 160,
  "Double Pana": 320,
  "Double Pana Bulk": 320,
  "DP Motor": 320,
  "Triple Pana": 1000,
  "Half Sangam": 1000,
  "Full Sangam": 10000,
  "SP DP TP": 320,
  "Odd Even": 10,
  "Panel Group": 160
};

export function options(request) {
  return corsPreflight(request);
}

async function requireAdmin(request) {
  const user = await requireUserByToken(getSessionToken(request));
  if (!user) {
    return { response: unauthorized(request) };
  }
  if (user.role !== "admin") {
    return { response: fail("Admin access required", 403, request) };
  }
  return { user };
}

function startOfTodayIso() {
  const now = new Date();
  now.setHours(0, 0, 0, 0);
  return now.toISOString();
}

function dayKey(value) {
  return String(value ?? "").slice(0, 10);
}

function lastNDates(days) {
  const dates = [];
  const current = new Date();
  current.setHours(0, 0, 0, 0);
  for (let index = days - 1; index >= 0; index -= 1) {
    const item = new Date(current);
    item.setDate(current.getDate() - index);
    dates.push(item.toISOString().slice(0, 10));
  }
  return dates;
}

function normalizeDate(value, fallback) {
  if (!value) {
    return fallback;
  }
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? fallback : parsed.toISOString();
}

function roundAmount(value) {
  return Math.round(Number(value || 0) * 100) / 100;
}

function isValidMarketResultString(result) {
  const value = String(result ?? "").trim();
  if (!/^[0-9*]{3}-[0-9*]{2}-[0-9*]{3}$/.test(value)) {
    return false;
  }

  return true;
}

function validateChartRows(rows, chartType) {
  if (!Array.isArray(rows) || rows.length === 0) {
    return "At least one chart row is required";
  }

  for (const row of rows) {
    if (!Array.isArray(row) || row.length < 2) {
      return "Every chart row must include a label and at least one value";
    }
    const values = row.map((cell) => String(cell ?? "").trim());
    if (!values[0]) {
      return "Every chart row must include a week label";
    }
    if (chartType === "jodi" && values.slice(1).some((value) => value && !/^(?:[0-9]{2,3}|[0-9]\*|\*\*|--)$/.test(value))) {
      return "Jodi chart values must be 2 digit values or bracket placeholders";
    }
    if (chartType === "panna" && values.slice(1).some((value) => value && !/^(?:[0-9]{3}|[0-9]\*\*|---|\*\*\*)$/.test(value))) {
      return "Panna chart values must be 3 digit values";
    }
  }

  return "";
}

function csvEscape(value) {
  const text = String(value ?? "");
  if (/[",\n]/.test(text)) {
    return `"${text.replace(/"/g, '""')}"`;
  }
  return text;
}

function toCsv(rows) {
  return rows.map((row) => row.map((cell) => csvEscape(cell)).join(",")).join("\n");
}

function parseResult(result) {
  const parts = String(result ?? "").split("-");
  const openPanna = parts[0] && /^[0-9]{3}$/.test(parts[0]) ? parts[0] : null;
  const jodi = parts[1] && /^[0-9]{2}$/.test(parts[1]) ? parts[1] : null;
  const closePanna = parts[2] && /^[0-9]{3}$/.test(parts[2]) ? parts[2] : null;
  const openAnk = parts[1] && /^[0-9]/.test(parts[1]) ? parts[1][0] : null;
  const closeAnk = parts[1] && /^[0-9*][0-9]$/.test(parts[1]) ? parts[1][1] : null;
  return { openPanna, jodi, closePanna, openAnk, closeAnk };
}

function getWeekStart(date) {
  const value = new Date(date);
  const day = value.getDay();
  const diff = day === 0 ? -6 : 1 - day;
  value.setDate(value.getDate() + diff);
  value.setHours(0, 0, 0, 0);
  return value;
}

function getWeekEnd(date) {
  const start = getWeekStart(date);
  const end = new Date(start);
  end.setDate(start.getDate() + 6);
  return end;
}

function formatChartDay(date) {
  const value = new Date(date);
  const month = value.toLocaleDateString("en-US", { month: "short" });
  const day = String(value.getDate()).padStart(2, "0");
  return `${month} ${day}`;
}

function getWeekChartLabel(date) {
  const start = getWeekStart(date);
  const end = getWeekEnd(date);
  return `${start.getFullYear()} ${formatChartDay(start)} to ${formatChartDay(end)}`;
}

function parseWeekLabelStartDate(label) {
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

function normalizeWeekLabel(label) {
  const parsed = parseWeekLabelStartDate(label);
  return parsed ? getWeekChartLabel(parsed) : String(label || "").trim();
}

function isPlaceholderChartValue(value) {
  const text = String(value || "").trim();
  return !text || text === "**" || text === "***" || text === "--" || text === "---";
}

function normalizeAndMergeChartRows(rows, size, placeholderFactory) {
  const merged = new Map();

  for (const row of Array.isArray(rows) ? rows : []) {
    if (!Array.isArray(row) || row.length === 0) {
      continue;
    }

    const label = normalizeWeekLabel(row[0]);
    const base = merged.get(label) ?? [label, ...Array.from({ length: size }, (_, index) => placeholderFactory(index))];

    for (let index = 0; index < size; index += 1) {
      const candidate = String(row[index + 1] ?? "").trim();
      if (!isPlaceholderChartValue(candidate)) {
        base[index + 1] = candidate;
      }
    }

    merged.set(label, base);
  }

  return Array.from(merged.values());
}

function getWeekdayIndex(date) {
  const day = new Date(date).getDay();
  return day === 0 ? 6 : day - 1;
}

function getOrCreateChartRow(rows, label, size, placeholderFactory) {
  const normalizedLabel = normalizeWeekLabel(label);
  const nextRows = normalizeAndMergeChartRows(rows, size, placeholderFactory).map((row) => [...row]);
  let index = nextRows.findIndex((row) => String(row?.[0] ?? "").trim() === normalizedLabel);
  if (index === -1) {
    const created = [normalizedLabel];
    for (let item = 0; item < size; item += 1) {
      created.push(placeholderFactory(item));
    }
    nextRows.push(created);
    index = nextRows.length - 1;
  } else if (nextRows[index].length < size + 1) {
    for (let item = nextRows[index].length - 1; item < size; item += 1) {
      nextRows[index].push(placeholderFactory(item));
    }
  }
  return { rows: nextRows, rowIndex: index };
}

function getChartRowSortKey(label) {
  const parsed = parseWeekLabelStartDate(label);
  return parsed ? parsed.getTime() : Number.MAX_SAFE_INTEGER;
}

function sortChartRowsChronologically(rows) {
  return [...rows].sort((left, right) => getChartRowSortKey(left?.[0]) - getChartRowSortKey(right?.[0]));
}

function sumDigitString(value) {
  return String(value || "")
    .split("")
    .reduce((total, digit) => total + Number(digit || 0), 0);
}

function deriveJodiRowsFromPannaRows(rows) {
  return sortChartRowsChronologically(
    (Array.isArray(rows) ? rows : []).map((row, rowIndex) => {
      const label = String(row?.[0] ?? `Week ${rowIndex + 1}`).trim();
      const nextRow = [label];
      for (let dayIndex = 0; dayIndex < 7; dayIndex += 1) {
        const open = String(row?.[1 + dayIndex * 2] ?? "").trim();
        const close = String(row?.[2 + dayIndex * 2] ?? "").trim();
        if (/^[0-9]{3}$/.test(open) && /^[0-9]{3}$/.test(close)) {
          nextRow.push(`${sumDigitString(open) % 10}${sumDigitString(close) % 10}`);
        } else if (/^[0-9]{3}$/.test(open) && /^[0-9]\*\*$/.test(close)) {
          nextRow.push(`${close[0]}*`);
        } else if (open === "***" || close === "***") {
          nextRow.push("**");
        } else {
          nextRow.push("--");
        }
      }
      return nextRow;
    })
  );
}

function normalizeChartRowsForSave(chartType, rows) {
  if (chartType === "panna") {
    return sortChartRowsChronologically(normalizeAndMergeChartRows(rows, 14, () => "---"));
  }
  return sortChartRowsChronologically(normalizeAndMergeChartRows(rows, 7, () => "--"));
}

async function syncChartsFromMarketResult(market) {
  const parsed = parseResult(market.result);
  const effectiveDate = new Date(market.updatedAt || Date.now());
  const label = getWeekChartLabel(effectiveDate);
  const weekdayIndex = getWeekdayIndex(effectiveDate);

  const jodiChart = await getChartRecord(market.slug, "jodi");
  const jodiRows = Array.isArray(jodiChart?.rows) ? jodiChart.rows : [];
  const jodiContainer = getOrCreateChartRow(jodiRows, label, 7, () => "**");
  if (market.result === "***-**-***") {
    jodiContainer.rows[jodiContainer.rowIndex][weekdayIndex + 1] = "**";
  } else if (parsed.openAnk && !parsed.jodi && !parsed.closePanna) {
    jodiContainer.rows[jodiContainer.rowIndex][weekdayIndex + 1] = `${parsed.openAnk}*`;
  } else if (parsed.jodi) {
    jodiContainer.rows[jodiContainer.rowIndex][weekdayIndex + 1] = parsed.jodi;
  }
  await upsertChartRecord(market.slug, "jodi", sortChartRowsChronologically(jodiContainer.rows));

  const pannaChart = await getChartRecord(market.slug, "panna");
  const pannaRows = Array.isArray(pannaChart?.rows) ? pannaChart.rows : [];
  const pannaContainer = getOrCreateChartRow(pannaRows, label, 14, () => "***");
  const openIndex = 1 + weekdayIndex * 2;
  const closeIndex = openIndex + 1;
  if (market.result === "***-**-***") {
    pannaContainer.rows[pannaContainer.rowIndex][openIndex] = "***";
    pannaContainer.rows[pannaContainer.rowIndex][closeIndex] = "***";
  } else {
    if (parsed.openPanna) {
      pannaContainer.rows[pannaContainer.rowIndex][openIndex] = parsed.openPanna;
    }
    if (parsed.closePanna) {
      pannaContainer.rows[pannaContainer.rowIndex][closeIndex] = parsed.closePanna;
    } else if (parsed.openAnk && !parsed.jodi) {
      pannaContainer.rows[pannaContainer.rowIndex][closeIndex] = `${parsed.openAnk}**`;
    }
  }
  await upsertChartRecord(market.slug, "panna", sortChartRowsChronologically(pannaContainer.rows));
}

function canSettleMarketResult(result) {
  const parsed = parseResult(result);
  return Boolean(parsed.openPanna || parsed.openAnk || parsed.jodi || parsed.closeAnk || parsed.closePanna);
}

function canEvaluateBidAgainstMarket(bid, parsed) {
  const board = bid.boardLabel;
  const sessionType = usesSession(board) ? bid.sessionType : "NA";

  if (["Single Digit", "Single Digit Bulk", "Odd Even"].includes(board)) {
    return sessionType === "Open" ? Boolean(parsed.openAnk) : Boolean(parsed.closeAnk);
  }

  if (["Single Pana", "Single Pana Bulk", "SP Motor", "Double Pana", "Double Pana Bulk", "DP Motor", "Choice Pana", "Triple Pana"].includes(board)) {
    return sessionType === "Open" ? Boolean(parsed.openPanna) : Boolean(parsed.closePanna);
  }

  if (["Jodi Digit", "Jodi Digit Bulk", "Group Jodi", "Red Bracket", "Digit Based Jodi", "Two Digit Panel (CP,SR)"].includes(board)) {
    return Boolean(parsed.jodi);
  }

  if (board === "Half Sangam") {
    return sessionType === "Open" ? Boolean(parsed.openPanna && parsed.closeAnk) : Boolean(parsed.openAnk && parsed.closePanna);
  }

  if (["SP DP TP", "Panel Group", "Full Sangam"].includes(board)) {
    return Boolean(parsed.openPanna && parsed.jodi && parsed.closePanna);
  }

  return Boolean(parsed.openPanna && parsed.jodi && parsed.closePanna);
}

function usesSession(board) {
  return ![
    "Jodi Digit",
    "Jodi Digit Bulk",
    "Group Jodi",
    "Red Bracket",
    "Digit Based Jodi",
    "Two Digit Panel (CP,SR)",
    "Panel Group",
    "SP DP TP",
    "Full Sangam"
  ].includes(board);
}

function isSingleDigitWin(board, digit, parsed, sessionType) {
  if (!["Single Digit", "Single Digit Bulk"].includes(board)) {
    return false;
  }
  return digit === (sessionType === "Open" ? parsed.openAnk : parsed.closeAnk);
}

function isJodiWin(board, digit, parsed) {
  if (!parsed.jodi) {
    return false;
  }
  if (["Jodi Digit", "Jodi Digit Bulk", "Red Bracket", "Two Digit Panel (CP,SR)"].includes(board)) {
    return digit === parsed.jodi;
  }
  if (board === "Group Jodi") {
    const [left, right] = digit.split("-");
    return left === parsed.jodi || right === parsed.jodi;
  }
  if (board === "Digit Based Jodi") {
    const prefix = digit.replace(/x$/i, "");
    return parsed.jodi.startsWith(prefix) || parsed.jodi.endsWith(prefix);
  }
  return false;
}

function isPanaWin(board, digit, parsed, sessionType) {
  const panel = sessionType === "Open" ? parsed.openPanna : parsed.closePanna;
  if (!panel) {
    return false;
  }
  if (["Single Pana", "Single Pana Bulk", "SP Motor"].includes(board)) {
    return panel === digit && getPannaType(digit) === "single";
  }
  if (["Double Pana", "Double Pana Bulk", "DP Motor"].includes(board)) {
    return panel === digit && getPannaType(digit) === "double";
  }
  if (board === "Choice Pana") {
    return panel === digit && isValidPanna(digit);
  }
  if (board === "Triple Pana") {
    return panel === digit && getPannaType(digit) === "triple";
  }
  return false;
}

function isSpDpTpWin(board, digit, parsed) {
  if (board !== "SP DP TP") {
    return false;
  }
  const types = [getPannaType(parsed.openPanna ?? ""), getPannaType(parsed.closePanna ?? "")];
  if (digit === "SP") return types.includes("single");
  if (digit === "DP") return types.includes("double");
  if (digit === "TP") return types.includes("triple");
  if (digit === "SP+DP") return types.includes("single") && types.includes("double");
  if (digit === "DP+TP") return types.includes("double") && types.includes("triple");
  if (digit === "SP+TP") return types.includes("single") && types.includes("triple");
  return false;
}

function isOddEvenWin(board, digit, parsed, sessionType) {
  if (board !== "Odd Even") {
    return false;
  }
  const openKind = parsed.openAnk ? (Number(parsed.openAnk) % 2 === 0 ? "Even" : "Odd") : null;
  const closeKind = parsed.closeAnk ? (Number(parsed.closeAnk) % 2 === 0 ? "Even" : "Odd") : null;
  if (openKind && closeKind && digit === `${openKind}-${closeKind}`) {
    return true;
  }
  return digit === (sessionType === "Open" ? openKind : closeKind);
}

function isPanelGroupWin(board, digit, parsed) {
  if (board !== "Panel Group") {
    return false;
  }
  if (/^[0-9]$/.test(digit)) {
    return [parsed.openPanna, parsed.closePanna].some((panel) => getSattaCardDigit(panel ?? "") === digit);
  }
  const [minRaw, maxRaw] = digit.split("-");
  const min = Number(minRaw);
  const max = Number(maxRaw);
  if (Number.isNaN(min) || Number.isNaN(max)) {
    return false;
  }
  return [parsed.openPanna, parsed.closePanna].some((panel) => {
    const value = Number(panel);
    return !Number.isNaN(value) && isValidPanna(panel ?? "") && value >= min && value <= max;
  });
}

function isSangamWin(board, digit, parsed, sessionType) {
  if (board === "Half Sangam") {
    const [first, second] = digit.split("-");
    if (!first || !second) {
      return false;
    }
    return sessionType === "Open" ? first === parsed.openPanna && second === parsed.closeAnk : first === parsed.openAnk && second === parsed.closePanna;
  }
  if (board === "Full Sangam") {
    const [openPanel, closePanel] = digit.split("-");
    return Boolean(openPanel && closePanel && openPanel === parsed.openPanna && closePanel === parsed.closePanna);
  }
  return false;
}

function evaluateBidAgainstMarket(bid, market) {
  const parsed = parseResult(market.result);
  const digit = String(bid.digit ?? "").trim();
  const board = bid.boardLabel;
  const sessionType = usesSession(board) ? bid.sessionType : "NA";

  if (!canEvaluateBidAgainstMarket(bid, parsed)) {
    return null;
  }

  const isWin =
    isSingleDigitWin(board, digit, parsed, sessionType) ||
    isJodiWin(board, digit, parsed) ||
    isPanaWin(board, digit, parsed, sessionType) ||
    isSpDpTpWin(board, digit, parsed) ||
    isOddEvenWin(board, digit, parsed, sessionType) ||
    isPanelGroupWin(board, digit, parsed) ||
    isSangamWin(board, digit, parsed, sessionType);

  return {
    status: isWin ? "Won" : "Lost",
    payout: isWin ? roundAmount(Number(bid.points ?? 0) * Number(payoutRates[board] ?? 0)) : 0
  };
}

async function settlePendingBidsForMarket(market) {
  if (!canSettleMarketResult(market.result)) {
    return { processed: 0, won: 0, lost: 0, wins: 0, losses: 0, skipped: 0, totalPayout: 0 };
  }

  const bids = (await getBidsForMarket(market.name)).filter((bid) => bid.status === "Pending");
  let processed = 0;
  let won = 0;
  let lost = 0;
  let skipped = 0;
  let totalPayout = 0;

  for (const bid of bids) {
    if (!payoutRates[bid.boardLabel]) {
      skipped += 1;
      continue;
    }
      const outcome = evaluateBidAgainstMarket(bid, market);
      if (!outcome) {
        skipped += 1;
        continue;
      }
      const updated = await updateBidSettlement(bid.id, outcome.status, outcome.payout, market.result);
      if (!updated) {
        skipped += 1;
        continue;
      }
      processed += 1;

      if (outcome.status === "Won" && outcome.payout > 0) {
      const beforeBalance = await getUserBalance(updated.userId);
      await addWalletEntry({
        userId: updated.userId,
        type: "BID_WIN",
        status: "SUCCESS",
        amount: outcome.payout,
        beforeBalance,
        afterBalance: beforeBalance + outcome.payout
      });
      won += 1;
      totalPayout += outcome.payout;
      } else {
        lost += 1;
      }
    }

  return { processed, won, lost, wins: won, losses: lost, skipped, totalPayout: roundAmount(totalPayout) };
}

async function resettleMarket(market) {
  const settled = (await getBidsForMarket(market.name)).filter((bid) => bid.status !== "Pending");

  for (const bid of settled) {
    if (bid.status === "Won" && bid.payout > 0) {
      const beforeBalance = await getUserBalance(bid.userId);
      const afterBalance = Math.max(0, beforeBalance - bid.payout);
      await addWalletEntry({
        userId: bid.userId,
        type: "BID_WIN_REVERSAL",
        status: "SUCCESS",
        amount: bid.payout,
        beforeBalance,
        afterBalance
      });
    }
    await updateBidSettlement(bid.id, "Pending", 0, "");
  }

  return settlePendingBidsForMarket(market);
}

export async function users(request) {
  const admin = await requireAdmin(request);
  if (admin.response) return admin.response;

  const usersList = await getUsersList();
  const snapshot = await getAdminSnapshot();
  const sevenDaysAgo = Date.now() - 7 * 24 * 60 * 60 * 1000;

  const data = await Promise.all(
    usersList.map(async (user) => {
      const lastSession = snapshot.sessions.find((session) => session.userId === user.id);
      const lastBid = snapshot.bids.find((bid) => bid.userId === user.id);
      const lastWalletEntry = snapshot.walletEntries.find((entry) => entry.userId === user.id);
      const lastActivity = [lastSession?.createdAt, lastBid?.createdAt, lastWalletEntry?.createdAt].filter(Boolean).sort().reverse()[0] ?? null;
      return {
        id: user.id,
        phone: user.phone,
        name: user.name,
        role: user.role,
        referralCode: user.referralCode,
        joinedAt: user.joinedAt,
        approvalStatus: user.approvalStatus,
        approvedAt: user.approvedAt,
        rejectedAt: user.rejectedAt,
        blockedAt: user.blockedAt,
        deactivatedAt: user.deactivatedAt,
        statusNote: user.statusNote,
        signupBonusGranted: user.signupBonusGranted,
        referredByUserId: user.referredByUserId,
        walletBalance: await getUserBalance(user.id),
        loginCount: snapshot.sessions.filter((session) => session.userId === user.id).length,
        bidCount: snapshot.bids.filter((bid) => bid.userId === user.id).length,
        totalBetAmount: snapshot.bids.filter((bid) => bid.userId === user.id).reduce((sum, bid) => sum + bid.points, 0),
        totalPayoutAmount: snapshot.walletEntries
          .filter((entry) => entry.userId === user.id && entry.type === "BID_WIN" && (entry.status === "SUCCESS" || entry.status === "BACKOFFICE"))
          .reduce((sum, entry) => sum + entry.amount, 0),
        lastActivity,
        activityState: lastActivity && new Date(lastActivity).getTime() >= sevenDaysAgo ? "Active" : "Inactive"
      };
    })
  );

  return ok(data, request);
}

export async function userDetail(request) {
  const admin = await requireAdmin(request);
  if (admin.response) return admin.response;

  const userId = String(new URL(request.url).searchParams.get("userId") ?? "");
  if (!userId) {
    return fail("userId is required", 400, request);
  }

  const user = await findUserById(userId);
  if (!user) {
    return fail("User not found", 404, request);
  }

  const [walletEntries, bids, bankAccounts, walletBalance] = await Promise.all([
    getWalletEntriesForUser(userId),
    getBidsForUser(userId),
    getBankAccountsForUser(userId),
    getUserBalance(userId)
  ]);

  return ok(
    {
      user: {
        id: user.id,
        name: user.name,
        phone: user.phone,
        role: user.role,
        referralCode: user.referralCode,
        joinedAt: user.joinedAt,
        approvalStatus: user.approvalStatus,
        approvedAt: user.approvedAt,
        rejectedAt: user.rejectedAt,
        blockedAt: user.blockedAt,
        deactivatedAt: user.deactivatedAt,
        statusNote: user.statusNote,
        signupBonusGranted: user.signupBonusGranted,
        walletBalance,
        referredByUserId: user.referredByUserId
      },
      bids,
      walletEntries,
      bankAccounts
    },
    request
  );
}

export async function userApproval(request) {
  const admin = await requireAdmin(request);
  if (admin.response) return admin.response;

  const body = await getJsonBody(request);
  const userId = String(body.userId ?? "");
  const action = String(body.action ?? "");
  const nextStatus = action === "approve" ? "Approved" : action === "reject" ? "Rejected" : null;
  if (!userId || !nextStatus) {
    return fail("userId and valid action are required", 400, request);
  }

  const updatedUser = await updateUserApprovalStatus(userId, nextStatus);
  if (!updatedUser) {
    return fail("User not found", 404, request);
  }

  await addAuditLog({
    actorUserId: admin.user.id,
    action: nextStatus === "Approved" ? "USER_APPROVED" : "USER_REJECTED",
    entityType: "user",
    entityId: updatedUser.id,
    details: JSON.stringify({
      phone: updatedUser.phone,
      approvalStatus: updatedUser.approvalStatus,
      signupBonusGranted: updatedUser.signupBonusGranted
    })
  });

  return ok({ user: updatedUser }, request);
}

export async function walletRequests(request) {
  const admin = await requireAdmin(request);
  if (admin.response) return admin.response;

  const requests = await getWalletApprovalRequests();
  const data = await Promise.all(
    requests.map(async (entry) => {
      const user = await findUserById(entry.userId);
      const bankAccounts = await getBankAccountsForUser(entry.userId);
      return {
        ...entry,
        user: user ? { id: user.id, phone: user.phone, name: user.name, approvalStatus: user.approvalStatus } : null,
        liveBalance: await getUserBalance(entry.userId),
        primaryBankAccount: bankAccounts[0] ?? null,
        referenceId: entry.referenceId || "",
        proofUrl: entry.proofUrl || "",
        note: entry.note || ""
      };
    })
  );

  return ok(data, request);
}

export async function walletRequestHistory(request) {
  const admin = await requireAdmin(request);
  if (admin.response) return admin.response;

  const items = await getWalletRequestHistory();
  const data = await Promise.all(
    items.map(async (entry) => {
      const user = await findUserById(entry.userId);
      const bankAccounts = await getBankAccountsForUser(entry.userId);
      return {
        ...entry,
        user: user ? { id: user.id, phone: user.phone, name: user.name, approvalStatus: user.approvalStatus } : null,
        liveBalance: await getUserBalance(entry.userId),
        primaryBankAccount: bankAccounts[0] ?? null,
        referenceId: entry.referenceId || "",
        proofUrl: entry.proofUrl || "",
        note: entry.note || ""
      };
    })
  );

  return ok(data, request);
}

export async function walletRequestAction(request) {
  const admin = await requireAdmin(request);
  if (admin.response) return admin.response;

  const body = await getJsonBody(request);
  const requestId = String(body.requestId ?? "");
  const action = String(body.action ?? "");
  const note = String(body.note ?? "").trim();
  const referenceId = String(body.referenceId ?? "").trim();
  const proofUrl = String(body.proofUrl ?? "").trim();
  if (!requestId || !["approve", "reject", "complete", "annotate"].includes(action)) {
    return fail("requestId and valid action are required", 400, request);
  }

  try {
    if (action === "complete" || action === "annotate") {
      const nextStatus = action === "complete" ? "SUCCESS" : undefined;
      const updated = await updateWalletEntryAdmin(requestId, {
        status: nextStatus,
        note,
        referenceId,
        proofUrl
      });
      if (!updated) {
        return fail("Wallet request not found", 404, request);
      }
      await addAuditLog({
        actorUserId: admin.user.id,
        action: action === "complete" ? "WALLET_REQUEST_COMPLETED" : "WALLET_REQUEST_ANNOTATED",
        entityType: "wallet_request",
        entityId: updated.id,
        details: JSON.stringify({
          type: updated.type,
          amount: updated.amount,
          status: updated.status,
          referenceId: updated.referenceId || null,
          proofUrl: updated.proofUrl || null,
          note: updated.note || null
        })
      });
      return ok({ request: updated, settlementEntry: null }, request);
    }

    const resolved = await resolveWalletApprovalRequest(requestId, action);
    if (!resolved?.request) {
      return fail("Wallet request not found", 404, request);
    }
    const patchedRequest = await updateWalletEntryAdmin(resolved.request.id, { note, referenceId, proofUrl });
    await addAuditLog({
      actorUserId: admin.user.id,
      action: action === "approve" ? "WALLET_REQUEST_APPROVED" : "WALLET_REQUEST_REJECTED",
      entityType: "wallet_request",
      entityId: patchedRequest?.id || resolved.request.id,
      details: JSON.stringify({
        type: resolved.request.type,
        amount: resolved.request.amount,
        settlementEntryId: resolved.settlementEntry?.id ?? null,
        referenceId: referenceId || null,
        proofUrl: proofUrl || null,
        note: note || null
      })
    });
    return ok({ request: patchedRequest || resolved.request, settlementEntry: resolved.settlementEntry }, request);
  } catch (error) {
    return fail(error instanceof Error ? error.message : "Unable to process wallet request", 400, request);
  }
}

export async function userStatus(request) {
  const admin = await requireAdmin(request);
  if (admin.response) return admin.response;

  const body = await getJsonBody(request);
  const userId = String(body.userId ?? "");
  const action = String(body.action ?? "");
  const note = String(body.note ?? "");

  if (!userId || !["block", "unblock", "deactivate", "activate"].includes(action)) {
    return fail("userId and valid action are required", 400, request);
  }

  const updatedUser = await updateUserAccountStatus(userId, action, note);
  if (!updatedUser) {
    return fail("User not found", 404, request);
  }

  await addAuditLog({
    actorUserId: admin.user.id,
    action: `USER_${action.toUpperCase()}`,
    entityType: "user",
    entityId: updatedUser.id,
    details: JSON.stringify({
      blockedAt: updatedUser.blockedAt,
      deactivatedAt: updatedUser.deactivatedAt,
      statusNote: updatedUser.statusNote
    })
  });

  return ok({ user: updatedUser }, request);
}

export async function walletAdjustment(request) {
  const admin = await requireAdmin(request);
  if (admin.response) return admin.response;

  const body = await getJsonBody(request);
  const userId = String(body.userId ?? "");
  const mode = String(body.mode ?? "").toLowerCase();
  const note = String(body.note ?? "").trim();
  const amount = roundAmount(Number(body.amount ?? 0));

  if (!userId || !["credit", "debit"].includes(mode) || amount <= 0) {
    return fail("userId, mode, and positive amount are required", 400, request);
  }

  const user = await findUserById(userId);
  if (!user) {
    return fail("User not found", 404, request);
  }

  const beforeBalance = await getUserBalance(userId);
  if (mode === "debit" && amount > beforeBalance) {
    return fail("Insufficient user balance for debit", 400, request);
  }

  const entry = await addWalletEntry({
    userId,
    type: mode === "credit" ? "ADMIN_CREDIT" : "ADMIN_DEBIT",
    status: "SUCCESS",
    amount,
    beforeBalance,
    afterBalance: mode === "credit" ? beforeBalance + amount : beforeBalance - amount
  });

  await addAuditLog({
    actorUserId: admin.user.id,
    action: mode === "credit" ? "WALLET_CREDIT" : "WALLET_DEBIT",
    entityType: "wallet_entry",
    entityId: entry.id,
    details: JSON.stringify({ userId, amount, note: note || null })
  });

  return ok({ entry }, request);
}

export async function notificationsList(request) {
  const admin = await requireAdmin(request);
  if (admin.response) return admin.response;
  return ok(await listAllNotifications(), request);
}

export async function notificationsSend(request) {
  const admin = await requireAdmin(request);
  if (admin.response) return admin.response;

  const body = await getJsonBody(request);
  const title = String(body.title ?? "").trim();
  const message = String(body.body ?? "").trim();
  const channel = String(body.channel ?? "general").trim() || "general";
  const userId = String(body.userId ?? "").trim();

  if (!title || !message) {
    return fail("title and body are required", 400, request);
  }

  const targets = userId ? [await findUserById(userId)].filter(Boolean) : (await getUsersList()).filter((user) => user.role !== "admin");
  if (!targets.length) {
    return fail("No notification targets found", 400, request);
  }

  const created = [];
  for (const target of targets) {
    created.push(await createNotification({ userId: target.id, title, body: message, channel }));
  }

  await addAuditLog({
    actorUserId: admin.user.id,
    action: "NOTIFICATION_SENT",
    entityType: "notification",
    entityId: userId || "broadcast",
    details: JSON.stringify({ title, channel, count: created.length })
  });

  return ok({ sent: created.length, items: created }, request);
}

export async function settingsGet(request) {
  const admin = await requireAdmin(request);
  if (admin.response) return admin.response;
  return ok(await getAppSettings(), request);
}

export async function settingsPublic(request) {
  const settings = await getAppSettings();
  const allowedKeys = new Set(["notice_text", "support_phone", "support_hours", "bonus_enabled", "bonus_text"]);
  return ok(settings.filter((item) => allowedKeys.has(item.key)), request);
}

export async function settingsUpdate(request) {
  const admin = await requireAdmin(request);
  if (admin.response) return admin.response;

  const body = await getJsonBody(request);
  const entries = Object.entries(body || {}).filter(([key]) => typeof key === "string" && key.trim());
  if (!entries.length) {
    return fail("At least one setting is required", 400, request);
  }

  const updated = [];
  for (const [key, value] of entries) {
    updated.push(await upsertAppSetting(key, String(value ?? "")));
  }

  await addAuditLog({
    actorUserId: admin.user.id,
    action: "SETTINGS_UPDATE",
    entityType: "settings",
    entityId: "app",
    details: JSON.stringify({ keys: updated.map((item) => item.key) })
  });

  return ok(updated, request);
}

export async function bidsList(request) {
  const admin = await requireAdmin(request);
  if (admin.response) return admin.response;

  const bids = await listAllBids();
  const data = await Promise.all(
    bids.map(async (bid) => {
      const user = await findUserById(bid.userId);
      return {
        ...bid,
        user: user ? { id: user.id, name: user.name, phone: user.phone } : null
      };
    })
  );

  return ok(data, request);
}

export async function auditLogs(request) {
  const admin = await requireAdmin(request);
  if (admin.response) return admin.response;
  return ok(await getAuditLogs(100), request);
}

export async function chartUpdate(request) {
  const admin = await requireAdmin(request);
  if (admin.response) return admin.response;

  const body = await getJsonBody(request);
  const slug = String(body.slug ?? "");
  const chartType = String(body.chartType ?? "jodi") === "panna" ? "panna" : "jodi";
  const rows = Array.isArray(body.rows) ? body.rows : [];
  if (!slug || rows.length === 0) {
    return fail("slug and rows are required", 400, request);
  }

  const normalizedRows = normalizeChartRowsForSave(
    chartType,
    rows.map((row) => (Array.isArray(row) ? row.map((cell) => String(cell ?? "")) : []))
  );
  const validationError = validateChartRows(normalizedRows, chartType);
  if (validationError) {
    return fail(validationError, 400, request);
  }
  const previousChart = await getChartRecord(slug, chartType);
  const previousRows = Array.isArray(previousChart?.rows) ? previousChart.rows : [];
  const updated = await upsertChartRecord(slug, chartType, normalizedRows);
  if (!updated) {
    return fail("Unable to update chart", 400, request);
  }

  if (chartType === "panna") {
    const derivedJodiRows = deriveJodiRowsFromPannaRows(normalizedRows);
    await upsertChartRecord(slug, "jodi", derivedJodiRows);
  }

  await addAuditLog({
    actorUserId: admin.user.id,
    action: "CHART_UPDATE",
    entityType: "chart",
    entityId: `${slug}:${chartType}`,
    details: JSON.stringify({
      rowCount: normalizedRows.length,
      previousRowCount: previousRows.length,
      previousRows,
      rows: normalizedRows
    })
  });

  return ok(updated, request);
}

export async function marketUpdate(request) {
  const admin = await requireAdmin(request);
  if (admin.response) return admin.response;

  const body = await getJsonBody(request);
  const slug = String(body.slug ?? "");
  const result = String(body.result ?? "");
  const status = String(body.status ?? "");
  const action = String(body.action ?? "");
  const open = String(body.open ?? "");
  const close = String(body.close ?? "");
  const category = String(body.category ?? "");
  if (!slug || !result || !status || !action || !open || !close || !category) {
    return fail("slug, result, status, action, open, close, and category are required", 400, request);
  }
  if (!isValidMarketResultString(result)) {
    return fail("Result must follow ***-**-***, 123-4*-***, or 123-45-678 format", 400, request);
  }

  const updated = await updateMarketRecord(slug, { result, status, action, open, close, category });
  if (!updated) {
    return fail("Market not found", 404, request);
  }

  await syncChartsFromMarketResult(updated);
  const settlement = await settlePendingBidsForMarket(updated);
  await addAuditLog({
    actorUserId: admin.user.id,
    action: "MARKET_UPDATE",
    entityType: "market",
    entityId: updated.slug,
    details: JSON.stringify({ result, status, action, open, close, category, settlement })
  });

  return ok({ market: updated, settlement }, request);
}

export async function settleMarket(request) {
  const admin = await requireAdmin(request);
  if (admin.response) return admin.response;

  const body = await getJsonBody(request);
  const slug = String(body.slug ?? "");
  const mode = String(body.mode ?? "settle");
  if (!slug) {
    return fail("slug is required", 400, request);
  }

  const market = await findMarketBySlug(slug);
  if (!market) {
    return fail("Market not found", 404, request);
  }

  const settlement = mode === "resettle" ? await resettleMarket(market) : await settlePendingBidsForMarket(market);
  await addAuditLog({
    actorUserId: admin.user.id,
    action: mode === "resettle" ? "MARKET_RESETTLE" : "MARKET_SETTLE",
    entityType: "market",
    entityId: market.slug,
    details: JSON.stringify(settlement)
  });

  return ok({ market, settlement }, request);
}

export async function settlementPreview(request) {
  const admin = await requireAdmin(request);
  if (admin.response) return admin.response;

  const slug = String(new URL(request.url).searchParams.get("slug") ?? "");
  if (!slug) {
    return fail("slug is required", 400, request);
  }

  const market = await findMarketBySlug(slug);
  if (!market) {
    return fail("Market not found", 404, request);
  }

  const bids = await getBidsForMarket(market.name);
  const previewItems = [];
  let eligible = 0;
  let wins = 0;
  let losses = 0;
  let pending = 0;
  let payout = 0;

  for (const bid of bids) {
    const outcome = evaluateBidAgainstMarket(bid, market);
    const user = await findUserById(bid.userId);
    if (!outcome) {
      pending += 1;
      if (previewItems.length < 20) {
        previewItems.push({
          id: bid.id,
          userName: user?.name ?? "Unknown",
          phone: user?.phone ?? "",
          boardLabel: bid.boardLabel,
          digit: bid.digit,
          sessionType: bid.sessionType,
          currentStatus: bid.status,
          previewStatus: "Pending",
          previewPayout: 0
        });
      }
      continue;
    }

    eligible += 1;
    if (outcome.status === "Won") {
      wins += 1;
      payout += outcome.payout;
    } else {
      losses += 1;
    }

    if (previewItems.length < 20) {
      previewItems.push({
        id: bid.id,
        userName: user?.name ?? "Unknown",
        phone: user?.phone ?? "",
        boardLabel: bid.boardLabel,
        digit: bid.digit,
        sessionType: bid.sessionType,
        currentStatus: bid.status,
        previewStatus: outcome.status,
        previewPayout: outcome.payout
      });
    }
  }

  return ok(
    {
      market: { slug: market.slug, name: market.name, result: market.result },
      summary: {
        totalBids: bids.length,
        eligible,
        pending,
        wins,
        losses,
        payout: roundAmount(payout)
      },
      items: previewItems
    },
    request
  );
}

export async function reconciliationSummary(request) {
  const admin = await requireAdmin(request);
  if (admin.response) return admin.response;

  const snapshot = await getAdminSnapshot();
  const staleCutoff = Date.now() - 24 * 60 * 60 * 1000;
  const walletRequests = snapshot.walletEntries.filter((entry) => ["DEPOSIT", "WITHDRAW"].includes(entry.type));
  const stalePending = walletRequests.filter((entry) => entry.status === "INITIATED" && new Date(entry.createdAt).getTime() < staleCutoff);
  const rejected = walletRequests.filter((entry) => entry.status === "REJECTED");
  const backoffice = walletRequests.filter((entry) => entry.status === "BACKOFFICE");
  const successful = walletRequests.filter((entry) => entry.status === "SUCCESS");

  const recent = await Promise.all(
    walletRequests.slice(0, 30).map(async (entry) => {
      const user = await findUserById(entry.userId);
      return {
        id: entry.id,
        type: entry.type,
        status: entry.status,
        amount: entry.amount,
        createdAt: entry.createdAt,
        userName: user?.name ?? "Unknown",
        phone: user?.phone ?? ""
      };
    })
  );

  return ok(
    {
      summary: {
        pendingCount: walletRequests.filter((entry) => entry.status === "INITIATED").length,
        stalePendingCount: stalePending.length,
        rejectedCount: rejected.length,
        backofficeCount: backoffice.length,
        successfulCount: successful.length,
        depositSuccessAmount: successful.filter((entry) => entry.type === "DEPOSIT").reduce((sum, entry) => sum + entry.amount, 0),
        withdrawSuccessAmount: successful.filter((entry) => entry.type === "WITHDRAW").reduce((sum, entry) => sum + entry.amount, 0)
      },
      recent
    },
    request
  );
}

export async function monitoringSummary(request) {
  const admin = await requireAdmin(request);
  if (admin.response) return admin.response;

  const snapshot = await getAdminSnapshot();
  const audit = await getAuditLogs(50);
  const blockedUsers = snapshot.users.filter((user) => user.blockedAt).length;
  const deactivatedUsers = snapshot.users.filter((user) => user.deactivatedAt).length;
  const pendingWithdraws = snapshot.walletEntries.filter((entry) => entry.type === "WITHDRAW" && entry.status === "INITIATED").length;
  const pendingDeposits = snapshot.walletEntries.filter((entry) => entry.type === "DEPOSIT" && entry.status === "INITIATED").length;
  const placeholderResults = snapshot.markets.filter((market) => market.result === "***-**-***").length;
  const staleAudits = audit.filter((item) => item.action.includes("REJECTED") || item.action.includes("RESET")).slice(0, 12);

  return ok(
    {
      summary: {
        blockedUsers,
        deactivatedUsers,
        pendingWithdraws,
        pendingDeposits,
        placeholderResults,
        auditEvents: audit.length
      },
      alerts: [
        pendingWithdraws > 0 ? { level: pendingWithdraws >= 5 ? "high" : "medium", title: "Pending withdraw queue", body: `${pendingWithdraws} withdraw requests are waiting.` } : null,
        pendingDeposits > 0 ? { level: "medium", title: "Pending deposit queue", body: `${pendingDeposits} deposit requests are waiting.` } : null,
        blockedUsers > 0 ? { level: "medium", title: "Blocked users present", body: `${blockedUsers} blocked users require review.` } : null,
        placeholderResults > 0 ? { level: "low", title: "Markets without results", body: `${placeholderResults} markets still show placeholder result strings.` } : null
      ].filter(Boolean),
      recentAuditFlags: staleAudits
    },
    request
  );
}

export async function exportData(request) {
  const admin = await requireAdmin(request);
  if (admin.response) return admin.response;

  const type = String(new URL(request.url).searchParams.get("type") ?? "users");
  let rows = [];

  if (type === "users") {
    const users = await getUsersList();
    rows = [
      ["id", "name", "phone", "role", "approvalStatus", "blockedAt", "deactivatedAt", "referralCode"],
      ...users.map((user) => [user.id, user.name, user.phone, user.role, user.approvalStatus, user.blockedAt ?? "", user.deactivatedAt ?? "", user.referralCode])
    ];
  } else if (type === "bids") {
    const bids = await listAllBids(1000);
    rows = [
      ["id", "userId", "market", "boardLabel", "sessionType", "digit", "points", "status", "payout", "createdAt"],
      ...bids.map((bid) => [bid.id, bid.userId, bid.market, bid.boardLabel, bid.sessionType, bid.digit, bid.points, bid.status, bid.payout, bid.createdAt])
    ];
  } else if (type === "requests") {
    const items = await getWalletRequestHistory();
    rows = [
      ["id", "userId", "type", "status", "amount", "referenceId", "proofUrl", "createdAt"],
      ...items.map((item) => [item.id, item.userId, item.type, item.status, item.amount, item.referenceId ?? "", item.proofUrl ?? "", item.createdAt])
    ];
  } else if (type === "audit") {
    const items = await getAuditLogs(1000);
    rows = [
      ["id", "actorUserId", "action", "entityType", "entityId", "createdAt"],
      ...items.map((item) => [item.id, item.actorUserId, item.action, item.entityType, item.entityId, item.createdAt])
    ];
  } else {
    return fail("Unsupported export type", 400, request);
  }

  const content = toCsv(rows);
  await addAuditLog({
    actorUserId: admin.user.id,
    action: "EXPORT_DATA",
    entityType: "export",
    entityId: type,
    details: JSON.stringify({ type, rowCount: rows.length - 1 })
  });

  return ok({ type, filename: `${type}-${Date.now()}.csv`, content, mimeType: "text/csv" }, request);
}

export async function backupSnapshot(request) {
  const admin = await requireAdmin(request);
  if (admin.response) return admin.response;

  const snapshot = await getAdminSnapshot();
  const settings = await getAppSettings();
  const charts = [];
  for (const market of snapshot.markets) {
    const jodi = await getChartRecord(market.slug, "jodi");
    const panna = await getChartRecord(market.slug, "panna");
    charts.push({ slug: market.slug, jodi: jodi?.rows ?? [], panna: panna?.rows ?? [] });
  }

  const payload = {
    generatedAt: new Date().toISOString(),
    version: 1,
    markets: snapshot.markets,
    settings,
    charts
  };

  await addAuditLog({
    actorUserId: admin.user.id,
    action: "BACKUP_EXPORT",
    entityType: "backup",
    entityId: "snapshot",
    details: JSON.stringify({ generatedAt: payload.generatedAt, markets: payload.markets.length, charts: payload.charts.length, settings: payload.settings.length })
  });

  return ok({ filename: `admin-backup-${Date.now()}.json`, snapshot: payload }, request);
}

export async function restoreSnapshot(request) {
  const admin = await requireAdmin(request);
  if (admin.response) return admin.response;

  const body = await getJsonBody(request);
  const snapshot = body?.snapshot;
  const dryRun = Boolean(body?.dryRun);
  if (!snapshot || typeof snapshot !== "object") {
    return fail("snapshot is required", 400, request);
  }

  const settings = Array.isArray(snapshot.settings) ? snapshot.settings : [];
  const markets = Array.isArray(snapshot.markets) ? snapshot.markets : [];
  const charts = Array.isArray(snapshot.charts) ? snapshot.charts : [];
  const chartErrors = [];

  for (const chart of charts) {
    const jodiError = validateChartRows(chart.jodi || [], "jodi");
    const pannaError = validateChartRows(chart.panna || [], "panna");
    if (jodiError) chartErrors.push(`${chart.slug}: ${jodiError}`);
    if (pannaError) chartErrors.push(`${chart.slug}: ${pannaError}`);
  }

  if (chartErrors.length) {
    return fail(chartErrors[0], 400, request);
  }

  if (!dryRun) {
    for (const item of settings) {
      await upsertAppSetting(String(item.key ?? ""), String(item.value ?? ""));
    }
    for (const market of markets) {
      await updateMarketRecord(String(market.slug ?? ""), {
        result: String(market.result ?? "***-**-***"),
        status: String(market.status ?? "Betting open now"),
        action: String(market.action ?? "Place Bet"),
        open: String(market.open ?? ""),
        close: String(market.close ?? ""),
        category: String(market.category ?? "main")
      });
    }
    for (const chart of charts) {
      await upsertChartRecord(String(chart.slug ?? ""), "jodi", chart.jodi || []);
      await upsertChartRecord(String(chart.slug ?? ""), "panna", chart.panna || []);
    }

    await addAuditLog({
      actorUserId: admin.user.id,
      action: "BACKUP_RESTORE",
      entityType: "backup",
      entityId: "snapshot",
      details: JSON.stringify({ settings: settings.length, markets: markets.length, charts: charts.length, dryRun: false })
    });
  }

  return ok(
    {
      dryRun,
      summary: {
        settings: settings.length,
        markets: markets.length,
        charts: charts.length
      }
    },
    request
  );
}

export async function dashboardSummary(request) {
  const admin = await requireAdmin(request);
  if (admin.response) return admin.response;

  const db = await getAdminSnapshot();
  const startOfToday = startOfTodayIso();
  const approvedUsers = db.users.filter((user) => user.approvalStatus === "Approved");
  const pendingUsers = db.users.filter((user) => user.approvalStatus === "Pending");
  const liveMarkets = db.markets.filter((market) => !market.action.toLowerCase().includes("closed"));
  const pendingWalletRequests = db.walletEntries.filter((entry) => entry.status === "INITIATED" && (entry.type === "DEPOSIT" || entry.type === "WITHDRAW"));
  const todayEntries = db.walletEntries.filter((entry) => entry.createdAt >= startOfToday);
  const todayDeposits = todayEntries.filter((entry) => entry.type === "DEPOSIT");
  const todayWithdraws = todayEntries.filter((entry) => entry.type === "WITHDRAW");
  const todayBonus = todayEntries.filter((entry) => entry.type === "SIGNUP_BONUS" && entry.status === "SUCCESS");
  const todayBids = db.bids.filter((bid) => bid.createdAt >= startOfToday);
  const todaySessions = db.sessions.filter((session) => session.createdAt >= startOfToday);
  const activeUserIds = new Set([...todaySessions.map((session) => session.userId), ...todayBids.map((bid) => bid.userId), ...todayEntries.map((entry) => entry.userId)]);
  const dateKeys = lastNDates(7);

  const collectionVsPayout = dateKeys.map((date) => ({
    date,
    collection: db.bids.filter((bid) => dayKey(bid.createdAt) === date).reduce((sum, bid) => sum + bid.points, 0),
    payout: db.walletEntries
      .filter((entry) => dayKey(entry.createdAt) === date && entry.type === "BID_WIN" && (entry.status === "SUCCESS" || entry.status === "BACKOFFICE"))
      .reduce((sum, entry) => sum + entry.amount, 0)
  }));

  const activeUsersTrend = dateKeys.map((date) => {
    const ids = new Set([
      ...db.sessions.filter((session) => dayKey(session.createdAt) === date).map((session) => session.userId),
      ...db.bids.filter((bid) => dayKey(bid.createdAt) === date).map((bid) => bid.userId),
      ...db.walletEntries.filter((entry) => dayKey(entry.createdAt) === date).map((entry) => entry.userId)
    ]);
    return { date, users: ids.size };
  });

  const topUsers = await Promise.all(
    approvedUsers.slice(0, 5).map(async (user) => ({
      id: user.id,
      name: user.name,
      phone: user.phone,
      balance: await getUserBalance(user.id)
    }))
  );

  const recentBids = await Promise.all(
    db.bids.slice(0, 8).map(async (bid) => {
      const user = await findUserById(bid.userId);
      return { id: bid.id, market: bid.market, boardLabel: bid.boardLabel, digit: bid.digit, points: bid.points, status: bid.status, createdAt: bid.createdAt, userName: user?.name ?? "Unknown", userPhone: user?.phone ?? "" };
    })
  );

  const recentRequests = await Promise.all(
    pendingWalletRequests.slice(0, 8).map(async (entry) => {
      const user = await findUserById(entry.userId);
      return { id: entry.id, type: entry.type, amount: entry.amount, createdAt: entry.createdAt, userName: user?.name ?? "Unknown", userPhone: user?.phone ?? "" };
    })
  );

  return ok(
    {
      totals: {
        users: db.users.length,
        approvedUsers: approvedUsers.length,
        pendingUsers: pendingUsers.length,
        pendingWalletRequests: pendingWalletRequests.length,
        markets: db.markets.length,
        liveMarkets: liveMarkets.length,
        deviceRegistrations: db.notificationDevices.length
      },
      today: {
        depositAmount: todayDeposits.filter((entry) => entry.status === "SUCCESS" || entry.status === "BACKOFFICE").reduce((sum, entry) => sum + entry.amount, 0),
        depositRequests: todayDeposits.filter((entry) => entry.status === "INITIATED").length,
        withdrawAmount: todayWithdraws.filter((entry) => entry.status === "SUCCESS" || entry.status === "BACKOFFICE").reduce((sum, entry) => sum + entry.amount, 0),
        withdrawRequests: todayWithdraws.filter((entry) => entry.status === "INITIATED").length,
        signupBonusAmount: todayBonus.reduce((sum, entry) => sum + entry.amount, 0),
        betsCount: todayBids.length,
        betsAmount: todayBids.reduce((sum, bid) => sum + bid.points, 0),
        loginCount: todaySessions.length,
        activeUsers: activeUserIds.size
      },
      trends: { collectionVsPayout, activeUsersTrend },
      pendingWork: {
        userApprovals: pendingUsers.length,
        walletApprovals: pendingWalletRequests.length,
        pendingDeposits: pendingWalletRequests.filter((entry) => entry.type === "DEPOSIT").length,
        pendingWithdraws: pendingWalletRequests.filter((entry) => entry.type === "WITHDRAW").length
      },
      topUsers,
      recentBids,
      recentRequests
    },
    request
  );
}

export async function reportsSummary(request) {
  const admin = await requireAdmin(request);
  if (admin.response) return admin.response;

  const url = new URL(request.url);
  const from = normalizeDate(url.searchParams.get("from"), startOfTodayIso());
  const to = normalizeDate(url.searchParams.get("to"), new Date().toISOString());
  const db = await getAdminSnapshot();
  const walletEntries = db.walletEntries.filter((entry) => entry.createdAt >= from && entry.createdAt <= to);
  const bids = db.bids.filter((bid) => bid.createdAt >= from && bid.createdAt <= to);
  const sessions = db.sessions.filter((session) => session.createdAt >= from && session.createdAt <= to);
  const deposits = walletEntries.filter((entry) => entry.type === "DEPOSIT");
  const withdraws = walletEntries.filter((entry) => entry.type === "WITHDRAW");
  const payouts = walletEntries.filter((entry) => entry.type === "BID_WIN");
  const marketTotals = new Map();
  const userTotals = new Map();

  for (const bid of bids) {
    const previous = userTotals.get(bid.userId) ?? { bidsCount: 0, betAmount: 0, payoutAmount: 0 };
    previous.bidsCount += 1;
    previous.betAmount += bid.points;
    previous.payoutAmount += bid.payout;
    userTotals.set(bid.userId, previous);

    const marketPrevious = marketTotals.get(bid.market) ?? { betsCount: 0, betsAmount: 0, payoutAmount: 0 };
    marketPrevious.betsCount += 1;
    marketPrevious.betsAmount += bid.points;
    marketPrevious.payoutAmount += bid.payout;
    marketTotals.set(bid.market, marketPrevious);
  }

  const userReports = await Promise.all(
    Array.from(userTotals.entries()).map(async ([userId, totals]) => {
      const user = await findUserById(userId);
      return { userId, userName: user?.name ?? "Unknown", userPhone: user?.phone ?? "", bidsCount: totals.bidsCount, betAmount: totals.betAmount, payoutAmount: totals.payoutAmount };
    })
  );
  userReports.sort((a, b) => b.betAmount - a.betAmount);

  const marketReports = Array.from(marketTotals.entries())
    .map(([market, totals]) => ({ market, betsCount: totals.betsCount, betsAmount: totals.betsAmount, payoutAmount: totals.payoutAmount }))
    .sort((a, b) => b.betsAmount - a.betsAmount);

  const dailySeriesMap = new Map();
  for (const bid of bids) {
    const key = dayKey(bid.createdAt);
    const previous = dailySeriesMap.get(key) ?? { collection: 0, payout: 0 };
    previous.collection += bid.points;
    dailySeriesMap.set(key, previous);
  }
  for (const entry of payouts) {
    const key = dayKey(entry.createdAt);
    const previous = dailySeriesMap.get(key) ?? { collection: 0, payout: 0 };
    previous.payout += entry.amount;
    dailySeriesMap.set(key, previous);
  }

  const dailySeries = Array.from(dailySeriesMap.entries())
    .map(([date, values]) => ({ date, collection: values.collection, payout: values.payout }))
    .sort((a, b) => a.date.localeCompare(b.date));

  return ok(
    {
      range: { from, to },
      totals: {
        depositsSuccess: deposits.filter((entry) => entry.status === "SUCCESS" || entry.status === "BACKOFFICE").reduce((sum, entry) => sum + entry.amount, 0),
        depositsPending: deposits.filter((entry) => entry.status === "INITIATED").reduce((sum, entry) => sum + entry.amount, 0),
        withdrawsSuccess: withdraws.filter((entry) => entry.status === "SUCCESS" || entry.status === "BACKOFFICE").reduce((sum, entry) => sum + entry.amount, 0),
        withdrawsPending: withdraws.filter((entry) => entry.status === "INITIATED").reduce((sum, entry) => sum + entry.amount, 0),
        withdrawsRejected: withdraws.filter((entry) => entry.status === "REJECTED").reduce((sum, entry) => sum + entry.amount, 0),
        betsCount: bids.length,
        betsAmount: bids.reduce((sum, bid) => sum + bid.points, 0),
        payoutAmount: payouts.reduce((sum, entry) => sum + entry.amount, 0),
        loginCount: sessions.length,
        activeUsers: new Set([...bids.map((item) => item.userId), ...walletEntries.map((item) => item.userId), ...sessions.map((item) => item.userId)]).size,
        collectionVsPayoutDelta: bids.reduce((sum, bid) => sum + bid.points, 0) - payouts.reduce((sum, entry) => sum + entry.amount, 0)
      },
      userReports,
      marketReports,
      dailySeries
    },
    request
  );
}
