import { Bid, Market } from "@/services/backend-service/core/schema";
import { getPannaSingleDigit, getPannaType, getSattaCardDigit, isValidPanna } from "@/services/backend-service/core/matka-rules";

type SettlementOutcome = {
  status: "Won" | "Lost";
  payout: number;
  reason: string;
};

type ParsedResult = {
  openPanna: string | null;
  jodi: string | null;
  closePanna: string | null;
  openAnk: string | null;
  closeAnk: string | null;
  digits: string[];
  jodiDigits: string[];
};

const payoutRates: Record<string, number> = {
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

export function canSettleMarketResult(result: string) {
  const parsed = parseResult(result);
  return Boolean(parsed.openPanna && parsed.jodi && parsed.closePanna);
}

export function evaluateBidAgainstMarket(bid: Bid, market: Market): SettlementOutcome {
  const parsed = parseResult(market.result);
  const board = bid.boardLabel;
  const digit = bid.digit.trim();
  const points = bid.points;
  const sessionType = bid.sessionType;

  if (!parsed.openPanna || !parsed.jodi || !parsed.closePanna) {
    return { status: "Lost", payout: 0, reason: "Incomplete result" };
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
    payout: isWin ? roundAmount(points * (payoutRates[board] ?? 0)) : 0,
    reason: isWin ? "Matched result" : "No matching result"
  };
}

function parseResult(result: string): ParsedResult {
  const parts = result.split("-");
  const openPanna = parts[0] && /^[0-9]{3}$/.test(parts[0]) ? parts[0] : null;
  const jodi = parts[1] && /^[0-9]{2}$/.test(parts[1]) ? parts[1] : null;
  const closePanna = parts[2] && /^[0-9]{3}$/.test(parts[2]) ? parts[2] : null;
  const openAnk = jodi ? jodi[0] : null;
  const closeAnk = jodi ? jodi[1] : null;
  const digits = result.replace(/[^0-9]/g, "").split("");
  const jodiDigits = jodi ? jodi.split("") : [];

  return { openPanna, jodi, closePanna, openAnk, closeAnk, digits, jodiDigits };
}

function isSingleDigitWin(board: string, digit: string, parsed: ParsedResult, sessionType: "Open" | "Close") {
  if (!["Single Digit", "Single Digit Bulk"].includes(board)) {
    return false;
  }
  const targetDigit = sessionType === "Open" ? parsed.openAnk : parsed.closeAnk;
  return digit === targetDigit;
}

function isJodiWin(board: string, digit: string, parsed: ParsedResult) {
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

function isPanaWin(board: string, digit: string, parsed: ParsedResult, sessionType: "Open" | "Close") {
  const panel = sessionType === "Open" ? parsed.openPanna : parsed.closePanna;
  const panels = panel ? [panel] : [];

  if (["Single Pana", "Single Pana Bulk", "SP Motor"].includes(board)) {
    return panels.includes(digit) && getPannaType(digit) === "single";
  }

  if (["Double Pana", "Double Pana Bulk", "DP Motor"].includes(board)) {
    return panels.includes(digit) && getPannaType(digit) === "double";
  }

  if (board === "Choice Pana") {
    return panels.includes(digit) && isValidPanna(digit);
  }

  if (board === "Triple Pana") {
    return panels.includes(digit) && getPannaType(digit) === "triple";
  }

  return false;
}

function isSpDpTpWin(board: string, digit: string, parsed: ParsedResult) {
  if (board !== "SP DP TP") {
    return false;
  }

  const types = [getPannaType(parsed.openPanna ?? ""), getPannaType(parsed.closePanna ?? "")];
  if (digit === "SP") {
    return types.includes("single");
  }
  if (digit === "DP") {
    return types.includes("double");
  }
  if (digit === "TP") {
    return types.includes("triple");
  }
  if (digit === "SP+DP") {
    return types.includes("single") && types.includes("double");
  }
  if (digit === "DP+TP") {
    return types.includes("double") && types.includes("triple");
  }
  if (digit === "SP+TP") {
    return types.includes("single") && types.includes("triple");
  }
  return false;
}

function isOddEvenWin(board: string, digit: string, parsed: ParsedResult, sessionType: "Open" | "Close") {
  if (board !== "Odd Even" || !parsed.jodi) {
    return false;
  }

  const openKind = Number(parsed.jodi[0]) % 2 === 0 ? "Even" : "Odd";
  const closeKind = Number(parsed.jodi[1]) % 2 === 0 ? "Even" : "Odd";

  if (digit === `${openKind}-${closeKind}`) {
    return true;
  }

  return digit === (sessionType === "Open" ? openKind : closeKind);
}

function isPanelGroupWin(board: string, digit: string, parsed: ParsedResult) {
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

function isSangamWin(board: string, digit: string, parsed: ParsedResult, sessionType: "Open" | "Close") {
  if (!parsed.jodi) {
    return false;
  }

  if (board === "Half Sangam") {
    const [first, second] = digit.split("-");
    if (!first || !second) {
      return false;
    }
    if (sessionType === "Open") {
      return first === parsed.openAnk && second === parsed.closePanna;
    }
    return first === parsed.openPanna && second === parsed.closeAnk;
  }

  if (board === "Full Sangam") {
    const [openPanel, closePanel] = digit.split("-");
    if (!openPanel || !closePanel) {
      return false;
    }
    return openPanel === parsed.openPanna && closePanel === parsed.closePanna;
  }

  return false;
}

export function explainPanna(value: string) {
  return {
    isValid: isValidPanna(value),
    panaType: getPannaType(value),
    singleDigit: getPannaSingleDigit(value),
    cardDigit: getSattaCardDigit(value)
  };
}

function roundAmount(value: number) {
  return Math.round(value * 100) / 100;
}

