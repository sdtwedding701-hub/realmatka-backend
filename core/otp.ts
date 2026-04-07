import { productionConfig } from "@/services/backend-service/core/config";
import { createNumericOtp, hashCredential } from "@/services/backend-service/core/security";
import { consumeOtpChallenge, createOtpChallenge } from "@/services/backend-service/core/store";

type OtpPurpose = "login" | "password_reset";

export async function sendOtp(phone: string, purpose: OtpPurpose) {
  const normalizedPhone = normalizeIndianPhone(phone);

  if (productionConfig.otpProvider === "twilio") {
    await sendTwilioOtp(normalizedPhone);
    return {
      sent: true,
      provider: "twilio" as const,
      expiresAt: new Date(Date.now() + 10 * 60 * 1000).toISOString(),
      devCode: null as string | null
    };
  }

  const otp = createNumericOtp(6);
  const expiresAt = new Date(Date.now() + 5 * 60 * 1000).toISOString();
  await createOtpChallenge({
    phone: phone.trim(),
    purpose,
    codeHash: hashCredential(otp),
    expiresAt
  });

  return {
    sent: true,
    provider: "local" as const,
    expiresAt,
    devCode: otp
  };
}

export async function verifyOtp(phone: string, purpose: OtpPurpose, code: string) {
  if (productionConfig.otpProvider === "twilio") {
    return verifyTwilioOtp(normalizeIndianPhone(phone), code);
  }

  return Boolean(await consumeOtpChallenge(phone, purpose, code));
}

function normalizeIndianPhone(phone: string) {
  const digits = phone.replace(/\D/g, "");
  if (digits.length === 10) {
    return `+91${digits}`;
  }
  if (digits.length === 12 && digits.startsWith("91")) {
    return `+${digits}`;
  }
  if (digits.startsWith("+")) {
    return digits;
  }
  return `+${digits}`;
}

async function sendTwilioOtp(phone: string) {
  const { twilioAccountSid, twilioAuthToken, twilioVerifyServiceSid } = productionConfig;
  if (!twilioAccountSid || !twilioAuthToken || !twilioVerifyServiceSid) {
    throw new Error("Twilio OTP is not configured. Set TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, and TWILIO_VERIFY_SERVICE_SID.");
  }

  const credentials = Buffer.from(`${twilioAccountSid}:${twilioAuthToken}`).toString("base64");
  const body = new URLSearchParams({
    To: phone,
    Channel: "sms"
  });

  const response = await fetch(`https://verify.twilio.com/v2/Services/${twilioVerifyServiceSid}/Verifications`, {
    method: "POST",
    headers: {
      Authorization: `Basic ${credentials}`,
      "Content-Type": "application/x-www-form-urlencoded"
    },
    body
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`Unable to send OTP via Twilio. ${errorText}`);
  }
}

async function verifyTwilioOtp(phone: string, code: string) {
  const { twilioAccountSid, twilioAuthToken, twilioVerifyServiceSid } = productionConfig;
  if (!twilioAccountSid || !twilioAuthToken || !twilioVerifyServiceSid) {
    throw new Error("Twilio OTP is not configured. Set TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, and TWILIO_VERIFY_SERVICE_SID.");
  }

  const credentials = Buffer.from(`${twilioAccountSid}:${twilioAuthToken}`).toString("base64");
  const body = new URLSearchParams({
    To: phone,
    Code: code
  });

  const response = await fetch(`https://verify.twilio.com/v2/Services/${twilioVerifyServiceSid}/VerificationCheck`, {
    method: "POST",
    headers: {
      Authorization: `Basic ${credentials}`,
      "Content-Type": "application/x-www-form-urlencoded"
    },
    body
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`Unable to verify OTP via Twilio. ${errorText}`);
  }

  const payload = (await response.json()) as { status?: string; valid?: boolean };
  return payload.valid === true || payload.status === "approved";
}

