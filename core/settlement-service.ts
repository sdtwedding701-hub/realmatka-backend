import { canSettleMarketResult, evaluateBidAgainstMarket } from "@/services/backend-service/core/settlement";
import { Bid, Market } from "@/services/backend-service/core/schema";
import { addWalletEntry, getBidsForMarket, getDb, getUserBalance, updateBidSettlement } from "@/services/backend-service/core/store";

export type SettlementSummary = {
  processed: number;
  won: number;
  lost: number;
  totalPayout: number;
};

export async function settlePendingBidsForMarket(market: Market): Promise<SettlementSummary> {
  if (!canSettleMarketResult(market.result)) {
    return { processed: 0, won: 0, lost: 0, totalPayout: 0 };
  }

  const bids = (await getBidsForMarket(market.name)).filter((bid: Bid) => bid.status === "Pending");
  let won = 0;
  let lost = 0;
  let totalPayout = 0;

  for (const bid of bids) {
    const outcome = evaluateBidAgainstMarket(bid, market);
    const updated = await updateBidSettlement(bid.id, outcome.status, outcome.payout, market.result);

    if (!updated) {
      continue;
    }

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

  return {
    processed: bids.length,
    won,
    lost,
    totalPayout: Math.round(totalPayout * 100) / 100
  };
}

export async function resettleMarket(market: Market): Promise<SettlementSummary> {
  const db = await getDb();
  const settled = db.bids.filter((bid) => bid.market === market.name && bid.status !== "Pending");

  for (const bid of settled) {
    if (bid.status === "Won" && bid.payout > 0) {
      const beforeBalance = await getUserBalance(bid.userId);
      await addWalletEntry({
        userId: bid.userId,
        type: "BID_PLACED",
        status: "SUCCESS",
        amount: bid.payout,
        beforeBalance,
        afterBalance: Math.max(0, beforeBalance - bid.payout)
      });
    }

    await updateBidSettlement(bid.id, "Pending", 0, "");
  }

  return settlePendingBidsForMarket(market);
}

