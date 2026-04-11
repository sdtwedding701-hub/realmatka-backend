import { createHmac, randomBytes } from "node:crypto";
import {
  completePaymentOrder,
  completePaymentLinkOrder,
  createPaymentOrder,
  findPaymentOrderByReferenceForUser,
  findWalletEntryByReferenceId,
  handlePaymentWebhook,
  findPaymentOrderForCheckout,
  findUserById,
  getUserBalance,
  requireUserByToken
} from "../db.mjs";
import { addWalletEntry, updateWalletEntryAdmin } from "../db.mjs";
import { corsPreflight, fail, getJsonBody, getSessionToken, ok, unauthorized } from "../http.mjs";
import { standaloneConfig } from "../config.mjs";

const razorpayKeyId = process.env.RAZORPAY_KEY_ID?.trim() || "";
const razorpayKeySecret = process.env.RAZORPAY_KEY_SECRET?.trim() || "";
const razorpayWebhookSecret = process.env.RAZORPAY_WEBHOOK_SECRET?.trim() || "";

function isRazorpayEnabled() {
  return Boolean(razorpayKeyId && razorpayKeySecret);
}

function getServerOrigin(request) {
  const requestUrl = new URL(request.url);
  const configuredOrigin =
    process.env.PAYMENTS_PUBLIC_ORIGIN?.trim() ||
    process.env.PUBLIC_API_ORIGIN?.trim() ||
    standaloneConfig.apiUrl;
  if (/^https?:\/\//i.test(configuredOrigin || "")) {
    return configuredOrigin.replace(/\/$/, "");
  }
  return requestUrl.origin.replace(/\/$/, "");
}

function roundToPaise(amount) {
  return Math.round(Number(amount || 0) * 100);
}

function getRazorpayAuthHeader() {
  return `Basic ${Buffer.from(`${razorpayKeyId}:${razorpayKeySecret}`).toString("base64")}`;
}

async function createRazorpayPaymentLink({ amountPaise, receipt, paymentOrderId, user }) {
  const appReturnBase = (standaloneConfig.appUrl || "https://play.realmatka.in").replace(/\/$/, "");
  const callbackUrl = `${appReturnBase}/wallet/payment-success?referenceId=${encodeURIComponent(receipt)}&amount=${encodeURIComponent(
    (amountPaise / 100).toFixed(2)
  )}`;

  const response = await fetch("https://api.razorpay.com/v1/payment_links", {
    method: "POST",
    headers: {
      Authorization: getRazorpayAuthHeader(),
      "Content-Type": "application/json"
    },
    body: JSON.stringify({
      amount: amountPaise,
      currency: "INR",
      upi_link: true,
      reference_id: receipt,
      description: "Real Matka Wallet Deposit",
      callback_url: callbackUrl,
      callback_method: "get",
      customer: {
        name: user?.name || "Real Matka User",
        contact: user?.phone ? `+91${user.phone}` : undefined
      },
      notes: {
        paymentOrderId,
        userId: user?.id || ""
      }
    })
  });

  const payload = await response.json().catch(() => null);
  if (!response.ok || !payload?.id || !payload?.short_url) {
    throw new Error(payload?.error?.description || payload?.description || "Unable to create Razorpay payment link");
  }

  return payload;
}

async function fetchRazorpayPaymentLinkStatus(paymentLinkId) {
  if (!paymentLinkId || !isRazorpayEnabled()) {
    return null;
  }

  const response = await fetch(`https://api.razorpay.com/v1/payment_links/${encodeURIComponent(paymentLinkId)}`, {
    method: "GET",
    headers: {
      Authorization: getRazorpayAuthHeader(),
      "Content-Type": "application/json"
    }
  });

  const payload = await response.json().catch(() => null);
  if (!response.ok || !payload?.id) {
    throw new Error(payload?.error?.description || payload?.description || "Unable to fetch Razorpay payment link status");
  }

  return payload;
}

function verifyRazorpaySignature({ orderId, paymentId, signature }) {
  const expected = createHmac("sha256", razorpayKeySecret).update(`${orderId}|${paymentId}`).digest("hex");
  return expected === signature;
}

function verifyRazorpayWebhookSignature(rawBody, signature) {
  if (!razorpayWebhookSecret || !signature) {
    return false;
  }
  const expected = createHmac("sha256", razorpayWebhookSecret).update(rawBody).digest("hex");
  return expected === signature;
}

function escapeHtml(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function renderHtml(content, status = 200) {
  return new Response(content, {
    status,
    headers: {
      "Content-Type": "text/html; charset=utf-8"
    }
  });
}

function buildHostedCheckoutHtml({ serverOrigin, paymentOrder, user, callbackUrl }) {
  const amountPaise = roundToPaise(paymentOrder.amount);
  const prefllPhone = user?.phone ? `+91${user.phone}` : "";
  const customerName = user?.name || "Real Matka User";
  const pageTitle = `Real Matka Deposit ${paymentOrder.reference}`;

  return `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>${escapeHtml(pageTitle)}</title>
    <style>
      body { margin: 0; font-family: Arial, sans-serif; background: #0b0b0b; color: #fff; display: flex; min-height: 100vh; align-items: center; justify-content: center; }
      .card { width: min(460px, calc(100vw - 32px)); background: #fff; color: #111; border-radius: 20px; padding: 28px; box-shadow: 0 30px 60px rgba(0,0,0,.35); }
      .eyebrow { font-size: 12px; letter-spacing: .18em; text-transform: uppercase; color: #666; }
      h1 { margin: 10px 0 8px; font-size: 28px; }
      p { margin: 0 0 14px; color: #555; line-height: 1.5; }
      .meta { background: #f5f5f5; border-radius: 14px; padding: 14px; margin: 18px 0; }
      .meta strong { display: block; font-size: 24px; color: #111; }
      button { width: 100%; min-height: 52px; border: 0; border-radius: 999px; background: #111; color: #fff; font-size: 16px; font-weight: 700; cursor: pointer; }
      .secondary { margin-top: 10px; background: #efefef; color: #111; }
      .help { margin-top: 16px; font-size: 13px; color: #666; text-align: center; }
    </style>
  </head>
  <body>
    <div class="card">
      <div class="eyebrow">Test Mode Deposit</div>
      <h1>Complete Wallet Deposit</h1>
      <p>You will be redirected to Razorpay secure checkout. Use Razorpay test UPI/card details to complete the payment.</p>
      <div class="meta">
        <span>Amount</span>
        <strong>Rs. ${escapeHtml(paymentOrder.amount.toFixed(2))}</strong>
        <span>Reference: ${escapeHtml(paymentOrder.reference)}</span>
      </div>
      <button id="pay-now">Pay Now</button>
      <button id="retry" class="secondary" type="button">Retry Checkout</button>
      <div class="help">If the checkout does not open automatically, tap Pay Now again.</div>
    </div>

    <script src="https://checkout.razorpay.com/v1/checkout.js"></script>
    <script>
      const options = {
        key: ${JSON.stringify(razorpayKeyId)},
        amount: ${JSON.stringify(String(amountPaise))},
        currency: "INR",
        name: "Real Matka",
        description: "Wallet Deposit",
        order_id: ${JSON.stringify(paymentOrder.gatewayOrderId)},
        callback_url: ${JSON.stringify(callbackUrl)},
        redirect: true,
        prefill: {
          name: ${JSON.stringify(customerName)},
          contact: ${JSON.stringify(prefllPhone)}
        },
        notes: {
          reference: ${JSON.stringify(paymentOrder.reference)},
          payment_order_id: ${JSON.stringify(paymentOrder.id)}
        },
        theme: {
          color: "#111111"
        },
        modal: {
          ondismiss: function () {
            document.querySelector(".help").textContent = "Checkout closed. Tap Retry Checkout to try again.";
          }
        }
      };

      const openCheckout = function () {
        const checkout = new Razorpay(options);
        checkout.open();
      };

      document.getElementById("pay-now").addEventListener("click", function (event) {
        event.preventDefault();
        openCheckout();
      });

      document.getElementById("retry").addEventListener("click", function (event) {
        event.preventDefault();
        openCheckout();
      });

      window.addEventListener("load", function () {
        setTimeout(openCheckout, 250);
      });
    </script>
  </body>
</html>`;
}

function buildPaymentResultHtml({ title, message, actionLabel, actionHref }) {
  return `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>${escapeHtml(title)}</title>
    <style>
      body { margin: 0; min-height: 100vh; display: flex; align-items: center; justify-content: center; background: #0b0b0b; font-family: Arial, sans-serif; color: #fff; }
      .card { width: min(420px, calc(100vw - 32px)); background: #fff; color: #111; border-radius: 20px; padding: 28px; text-align: center; }
      h1 { margin: 0 0 12px; font-size: 28px; }
      p { margin: 0 0 18px; color: #555; line-height: 1.5; }
      a { display: inline-flex; align-items: center; justify-content: center; min-height: 48px; padding: 0 20px; border-radius: 999px; background: #111; color: #fff; text-decoration: none; font-weight: 700; }
    </style>
  </head>
  <body>
    <div class="card">
      <h1>${escapeHtml(title)}</h1>
      <p>${escapeHtml(message)}</p>
      <a href="${escapeHtml(actionHref)}">${escapeHtml(actionLabel)}</a>
    </div>
  </body>
</html>`;
}

async function getCallbackPayload(request) {
  const contentType = request.headers.get("content-type") || "";
  if (/application\/x-www-form-urlencoded/i.test(contentType) || /multipart\/form-data/i.test(contentType)) {
    const form = await request.formData();
    return {
      razorpayPaymentId: String(form.get("razorpay_payment_id") ?? "").trim(),
      razorpayOrderId: String(form.get("razorpay_order_id") ?? "").trim(),
      razorpaySignature: String(form.get("razorpay_signature") ?? "").trim()
    };
  }

  const body = await getJsonBody(request);
  return {
    razorpayPaymentId: String(body.razorpay_payment_id ?? "").trim(),
    razorpayOrderId: String(body.razorpay_order_id ?? "").trim(),
    razorpaySignature: String(body.razorpay_signature ?? "").trim()
  };
}

export function options(request) {
  return corsPreflight(request);
}

export async function createOrder(request) {
  const user = await requireUserByToken(getSessionToken(request));
  if (!user) {
    return unauthorized(request);
  }
  if (!isRazorpayEnabled()) {
    return fail("Razorpay test mode keys are not configured", 503, request);
  }

  const body = await getJsonBody(request);
  const amount = Number(body.amount ?? 0);
  const platform = String(body.platform ?? "web").trim().toLowerCase();
  const amountPaise = roundToPaise(amount);
  if (amountPaise < 100) {
    return fail("Minimum deposit is Rs. 1", 400, request);
  }

  const paymentOrderId = `payment_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
  const reference = `RM${Date.now()}${Math.random().toString(36).slice(2, 4).toUpperCase()}`.slice(0, 40);
  const checkoutToken = randomBytes(24).toString("hex");
  const razorpayPaymentLink = await createRazorpayPaymentLink({
    amountPaise,
    receipt: reference,
    paymentOrderId,
    user
  });
  const redirectUrl = razorpayPaymentLink.short_url;

  const order = await createPaymentOrder({
    id: paymentOrderId,
    userId: user.id,
    amount,
    provider: "razorpay_payment_link",
    reference,
    checkoutToken,
    gatewayOrderId: razorpayPaymentLink.id,
    redirectUrl
  });

  return ok(order, request);
}

export async function getPaymentOrderStatus(request) {
  const user = await requireUserByToken(getSessionToken(request));
  if (!user) {
    return unauthorized(request);
  }

  const body = request.method.toUpperCase() === "GET" ? getRequestParams(request) : await getJsonBody(request);
  const referenceId = String(body.referenceId ?? body.reference ?? "").trim();
  if (!referenceId) {
    return fail("referenceId is required", 400, request);
  }

  let order = await findPaymentOrderByReferenceForUser(user.id, referenceId);
  if (!order) {
    return fail("Payment order not found", 404, request);
  }

  if (order.status === "PENDING" && order.provider === "razorpay_payment_link" && order.gatewayOrderId && isRazorpayEnabled()) {
    try {
      const paymentLink = await fetchRazorpayPaymentLinkStatus(order.gatewayOrderId);
      const remoteStatus = String(paymentLink?.status || "").trim().toLowerCase();

      if (remoteStatus === "paid") {
        order = await completePaymentLinkOrder({
          reference: order.reference,
          gatewayOrderId: String(order.gatewayOrderId || paymentLink.id || "").trim(),
          gatewayPaymentId: String(paymentLink.payment_id || paymentLink.payments?.[0]?.payment_id || paymentLink.payments?.[0]?.id || "").trim(),
          gatewaySignature: "payment_link_status_poll"
        });
      } else if (remoteStatus === "cancelled" || remoteStatus === "expired") {
        order = await handlePaymentWebhook(order.reference, "FAILED");
      } else {
        order = {
          ...order,
          remoteStatus: remoteStatus || "created"
        };
      }
    } catch (error) {
      return fail(error instanceof Error ? error.message : "Unable to verify payment status", 502, request);
    }
  }

  return ok(order, request);
}

function normalizeUpiClientStatus(value) {
  const status = String(value ?? "").trim().toUpperCase();
  if (status === "SUCCESS") {
    return "INITIATED";
  }
  if (status === "SUBMITTED") {
    return "INITIATED";
  }
  if (status === "FAILED") {
    return "FAILED";
  }
  if (status === "CANCELLED") {
    return "CANCELLED";
  }
  return "";
}

function getRequestParams(request) {
  const url = new URL(request.url);
  return Object.fromEntries(url.searchParams.entries());
}

export async function startUpiDeposit(request) {
  const user = await requireUserByToken(getSessionToken(request));
  if (!user) {
    return unauthorized(request);
  }

  const body = request.method.toUpperCase() === "GET" ? getRequestParams(request) : await getJsonBody(request);
  const amount = Number(body.amount ?? 0);
  const appName = String(body.appName ?? "UPI").trim() || "UPI";
  const referenceId = String(body.referenceId ?? "").trim();
  if (amount <= 0) {
    return fail("Amount must be greater than 0", 400, request);
  }
  if (!referenceId) {
    return fail("referenceId is required", 400, request);
  }

  const existing = await findWalletEntryByReferenceId(user.id, referenceId);
  if (existing) {
    return ok(existing, request);
  }

  const beforeBalance = await getUserBalance(user.id);
  const entry = await addWalletEntry({
    userId: user.id,
    type: "DEPOSIT",
    status: "INITIATED",
    amount,
    beforeBalance,
    afterBalance: beforeBalance,
    referenceId,
    note: JSON.stringify({
      channel: "upi_intent",
      appName,
      appReportedStatus: "STARTED"
    })
  });

  return ok(entry, request);
}

export async function reportUpiDeposit(request) {
  const user = await requireUserByToken(getSessionToken(request));
  if (!user) {
    return unauthorized(request);
  }

  const body = request.method.toUpperCase() === "GET" ? getRequestParams(request) : await getJsonBody(request);
  const referenceId = String(body.referenceId ?? "").trim();
  const appName = String(body.appName ?? "UPI").trim() || "UPI";
  const rawResponse = String(body.rawResponse ?? "").trim();
  const utr = String(body.utr ?? "").trim();
  const appReportedStatus = String(body.appReportedStatus ?? "").trim().toUpperCase();
  const mappedStatus = normalizeUpiClientStatus(appReportedStatus);

  if (!referenceId) {
    return fail("referenceId is required", 400, request);
  }
  if (!mappedStatus) {
    return fail("Unsupported appReportedStatus", 400, request);
  }
  const existing = await findWalletEntryByReferenceId(user.id, referenceId);
  if (!existing) {
    return fail("Deposit request not found", 404, request);
  }

  const nextNote = [
    `UPI App: ${appName}`,
    `Client Status: ${appReportedStatus}`,
    utr ? `UTR: ${utr}` : "",
    rawResponse ? `Raw: ${rawResponse}` : ""
  ]
    .filter(Boolean)
    .join(" | ");

  const updated = await updateWalletEntryAdmin(existing.id, {
    status: mappedStatus,
    referenceId: utr || referenceId,
    note: nextNote
  });

  return ok(updated, request);
}

export async function getUpiDepositStatus(request) {
  const user = await requireUserByToken(getSessionToken(request));
  if (!user) {
    return unauthorized(request);
  }

  const body = request.method.toUpperCase() === "GET" ? getRequestParams(request) : await getJsonBody(request);
  const referenceId = String(body.referenceId ?? "").trim();
  if (!referenceId) {
    return fail("referenceId is required", 400, request);
  }

  const existing = await findWalletEntryByReferenceId(user.id, referenceId);
  if (!existing) {
    return fail("Deposit request not found", 404, request);
  }

  return ok(existing, request);
}

export async function checkoutPage(request) {
  const url = new URL(request.url);
  const paymentOrderId = String(url.searchParams.get("paymentOrderId") ?? "").trim();
  const checkoutToken = String(url.searchParams.get("token") ?? "").trim();

  const paymentOrder = await findPaymentOrderForCheckout(paymentOrderId, checkoutToken);
  if (!paymentOrder) {
    return renderHtml(buildPaymentResultHtml({
      title: "Invalid Payment Link",
      message: "This deposit session is invalid or has expired. Please start a new payment from the app.",
      actionLabel: "Back to Website",
      actionHref: standaloneConfig.appUrl || "https://play.realmatka.in"
    }), 404);
  }

  const user = await findUserById(paymentOrder.userId);
  const serverOrigin = getServerOrigin(request);
  const callbackUrl = `${serverOrigin}/payments/callback?paymentOrderId=${encodeURIComponent(paymentOrder.id)}&token=${encodeURIComponent(checkoutToken)}&platform=${encodeURIComponent(
    String(url.searchParams.get("platform") ?? "web").trim().toLowerCase()
  )}`;

  return renderHtml(buildHostedCheckoutHtml({ serverOrigin, paymentOrder, user, callbackUrl }));
}

export async function callbackPage(request) {
  const url = new URL(request.url);
  const paymentOrderId = String(url.searchParams.get("paymentOrderId") ?? "").trim();
  const checkoutToken = String(url.searchParams.get("token") ?? "").trim();
  const platform = String(url.searchParams.get("platform") ?? "web").trim().toLowerCase();
  const paymentOrder = await findPaymentOrderForCheckout(paymentOrderId, checkoutToken);

  if (!paymentOrder) {
    return renderHtml(
      buildPaymentResultHtml({
        title: "Payment Session Invalid",
        message: "This payment session could not be verified. Please start a fresh deposit request.",
        actionLabel: "Back to Website",
        actionHref: standaloneConfig.appUrl || "https://play.realmatka.in"
      }),
      404
    );
  }

  const payload = await getCallbackPayload(request);
  if (!payload.razorpayPaymentId || !payload.razorpayOrderId || !payload.razorpaySignature) {
    return renderHtml(
      buildPaymentResultHtml({
        title: "Payment Incomplete",
        message: "Razorpay did not return a valid payment confirmation. Please retry the deposit.",
        actionLabel: "Retry Deposit",
        actionHref: paymentOrder.redirectUrl || standaloneConfig.appUrl || "https://play.realmatka.in"
      }),
      400
    );
  }

  if (!verifyRazorpaySignature({
    orderId: payload.razorpayOrderId,
    paymentId: payload.razorpayPaymentId,
    signature: payload.razorpaySignature
  })) {
    return renderHtml(
      buildPaymentResultHtml({
        title: "Payment Verification Failed",
        message: "The payment signature could not be verified. Your wallet was not credited.",
        actionLabel: "Back to Website",
        actionHref: standaloneConfig.appUrl || "https://play.realmatka.in"
      }),
      400
    );
  }

  await completePaymentOrder({
    paymentOrderId: paymentOrder.id,
    gatewayOrderId: payload.razorpayOrderId,
    gatewayPaymentId: payload.razorpayPaymentId,
    gatewaySignature: payload.razorpaySignature
  });

  const webReturnUrl = `${(standaloneConfig.appUrl || "https://play.realmatka.in").replace(/\/$/, "")}/wallet/history?payment=success&reference=${encodeURIComponent(
    paymentOrder.reference
  )}`;

  if (platform === "web") {
    return new Response(null, {
      status: 302,
      headers: {
        Location: webReturnUrl
      }
    });
  }

  return renderHtml(
    buildPaymentResultHtml({
      title: "Payment Successful",
      message: "Your wallet has been credited. You can now return to the app and refresh your wallet history.",
      actionLabel: "Open Web Wallet",
      actionHref: webReturnUrl
    })
  );
}

export async function webhook(request) {
  const rawBody = await request.text();
  const signature = request.headers.get("x-razorpay-signature")?.trim() || "";

  if (!razorpayWebhookSecret) {
    return fail("Razorpay webhook secret is not configured", 503, request);
  }

  if (!verifyRazorpayWebhookSignature(rawBody, signature)) {
    return fail("Invalid webhook signature", 400, request);
  }

  let body = {};
  try {
    body = rawBody ? JSON.parse(rawBody) : {};
  } catch {
    return fail("Invalid webhook payload", 400, request);
  }

  const event = String(body?.event || "").trim();
  const paymentLinkEntity = body?.payload?.payment_link?.entity || {};
  const orderEntity = body?.payload?.order?.entity || {};
  const paymentEntity = body?.payload?.payment?.entity || {};
  const reference =
    String(paymentLinkEntity.reference_id || paymentLinkEntity.reference || orderEntity.receipt || "").trim();
  const gatewayOrderId =
    String(paymentLinkEntity.id || paymentLinkEntity.order_id || orderEntity.order_id || orderEntity.id || "").trim();
  const gatewayPaymentId =
    String(paymentEntity.id || paymentLinkEntity.payments?.[0]?.payment_id || paymentLinkEntity.payments?.[0]?.id || orderEntity.payment_id || orderEntity.id || "").trim();

  if (event === "payment_link.paid") {
    const updated = await completePaymentLinkOrder({
      reference,
      gatewayOrderId,
      gatewayPaymentId,
      gatewaySignature: signature
    });

    if (!updated) {
      return fail("Payment link order not found", 404, request);
    }

    return ok({ received: true, event, status: "SUCCESS", order: updated }, request);
  }

  if (event === "payment_link.cancelled" || event === "payment_link.expired") {
    return ok({ received: true, event, status: "IGNORED" }, request);
  }

  return ok({ received: true, event, status: "IGNORED" }, request);
}
