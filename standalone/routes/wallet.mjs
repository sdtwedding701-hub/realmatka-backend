import { addWalletEntry, getBankAccountsForUser, getUserBalance, getWalletEntriesForUser, requireUserByToken } from "../db.mjs";
import { corsPreflight, fail, getJsonBody, getSessionToken, ok, unauthorized } from "../http.mjs";

export function options(request) {
  return corsPreflight(request);
}

export async function history(request) {
  const user = await requireUserByToken(getSessionToken(request));
  if (!user) {
    return unauthorized(request);
  }

  return ok(await getWalletEntriesForUser(user.id), request);
}

export async function deposit(request) {
  const user = await requireUserByToken(getSessionToken(request));
  if (!user) {
    return unauthorized(request);
  }

  const body = await getJsonBody(request);
  const amount = Number(body.amount ?? 0);
  if (amount <= 0) {
    return fail("Amount must be greater than 0", 400, request);
  }

  const beforeBalance = await getUserBalance(user.id);
  return ok(
    await addWalletEntry({
      userId: user.id,
      type: "DEPOSIT",
      status: "INITIATED",
      amount,
      beforeBalance,
      afterBalance: beforeBalance
    }),
    request
  );
}

export async function withdraw(request) {
  const user = await requireUserByToken(getSessionToken(request));
  if (!user) {
    return unauthorized(request);
  }

  const body = await getJsonBody(request);
  const amount = Number(body.amount ?? 0);
  const referenceId = String(body.referenceId ?? "").trim();
  const proofUrl = String(body.proofUrl ?? "").trim();
  const note = String(body.note ?? "").trim();
  if (amount < 500) {
    return fail("Minimum withdrawal is 500", 400, request);
  }

  const bankAccounts = await getBankAccountsForUser(user.id);
  if (!bankAccounts.length) {
    return fail("Add bank details before requesting a withdrawal", 400, request);
  }

  const beforeBalance = await getUserBalance(user.id);
  if (amount > beforeBalance) {
    return fail("Insufficient balance", 400, request);
  }

  return ok(
    await addWalletEntry({
      userId: user.id,
      type: "WITHDRAW",
      status: "INITIATED",
      amount,
      beforeBalance,
      afterBalance: beforeBalance,
      referenceId,
      proofUrl,
      note
    }),
    request
  );
}
