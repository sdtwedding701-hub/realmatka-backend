import { createHash, randomBytes, scryptSync, timingSafeEqual } from "node:crypto";

export function hashSecret(value: string) {
  return createHash("sha256").update(value).digest("hex");
}

export function hashCredential(value: string) {
  const salt = randomBytes(16).toString("hex");
  const derived = scryptSync(value, salt, 64).toString("hex");
  return `scrypt$${salt}$${derived}`;
}

export function verifyCredential(value: string, hashed: string) {
  if (hashed.startsWith("scrypt$")) {
    const [, salt, derived] = hashed.split("$");
    if (!salt || !derived) {
      return false;
    }
    const left = Buffer.from(scryptSync(value, salt, 64).toString("hex"));
    const right = Buffer.from(derived);
    return left.length === right.length && timingSafeEqual(left, right);
  }

  const left = Buffer.from(hashSecret(value));
  const right = Buffer.from(hashed);
  return left.length === right.length && timingSafeEqual(left, right);
}

export function verifySecret(value: string, hashed: string) {
  return verifyCredential(value, hashed);
}

export function createOpaqueToken() {
  return randomBytes(24).toString("hex");
}

export function createNumericOtp(length = 6) {
  let value = "";
  while (value.length < length) {
    value += randomBytes(length).toString("hex").replace(/\D/g, "");
  }
  return value.slice(0, length);
}
