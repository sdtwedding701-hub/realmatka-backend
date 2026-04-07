import { productionConfig } from "@/services/backend-service/core/config";
import { addPaymentOrder, updatePaymentOrderStatus } from "@/services/backend-service/core/store";

export function createPaymentOrder(input: { userId: string; amount: number }) {
  const reference = `pay_${Date.now()}`;
  const redirectUrl =
    productionConfig.paymentProvider === "manual"
      ? null
      : `${productionConfig.appUrl.replace(/\/$/, "")}/wallet/payment-redirect?ref=${reference}`;

  return addPaymentOrder({
    userId: input.userId,
    provider: normalizeProvider(productionConfig.paymentProvider),
    amount: input.amount,
    status: productionConfig.paymentProvider === "manual" ? "PENDING" : "CREATED",
    reference,
    redirectUrl
  });
}

export function handlePaymentWebhook(reference: string, status: "SUCCESS" | "FAILED" | "PENDING") {
  return updatePaymentOrderStatus(reference, status);
}

function normalizeProvider(value: string): "manual" | "razorpay" | "cashfree" | "phonepe" {
  if (value === "razorpay" || value === "cashfree" || value === "phonepe") {
    return value;
  }
  return "manual";
}

