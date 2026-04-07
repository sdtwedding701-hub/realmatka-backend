import { addBankAccount, getBankAccountsForUser, requireUserByToken } from "../db.mjs";
import { corsPreflight, fail, getJsonBody, getSessionToken, ok, unauthorized } from "../http.mjs";

export function options(request) {
  return corsPreflight(request);
}

export async function list(request) {
  const user = await requireUserByToken(getSessionToken(request));
  if (!user) {
    return unauthorized(request);
  }

  return ok(await getBankAccountsForUser(user.id), request);
}

export async function add(request) {
  const user = await requireUserByToken(getSessionToken(request));
  if (!user) {
    return unauthorized(request);
  }

  const body = await getJsonBody(request);
  const accountNumber = String(body.accountNumber ?? "");
  const holderName = String(body.holderName ?? "");
  const ifsc = String(body.ifsc ?? "");

  if (!accountNumber || !holderName || !ifsc) {
    return fail("accountNumber, holderName, and ifsc are required", 400, request);
  }

  return ok(await addBankAccount({ userId: user.id, accountNumber, holderName, ifsc }), request);
}
