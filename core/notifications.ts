import { productionConfig } from "@/services/backend-service/core/config";
import { addNotificationRecord, getDb, upsertNotificationDevice } from "@/services/backend-service/core/store";

export function registerNotificationDevice(userId: string, platform: "android" | "ios" | "web", token: string) {
  return upsertNotificationDevice({ userId, platform, token, enabled: true });
}

export async function listNotificationsForUser(userId: string) {
  return (await getDb()).notifications.filter((item) => item.userId === userId);
}

export function createSystemNotification(input: {
  userId: string;
  title: string;
  body: string;
  channel: "system" | "wallet" | "market" | "security" | "promotion";
}) {
  return addNotificationRecord({ ...input, read: false });
}

export async function dispatchNotification(input: {
  userId: string;
  title: string;
  body: string;
  channel: "system" | "wallet" | "market" | "security" | "promotion";
}) {
  const record = await createSystemNotification(input);
  const devices = (await getDb()).notificationDevices.filter((item) => item.userId === input.userId && item.enabled);

  return {
    provider: productionConfig.notificationProvider,
    dispatched: productionConfig.notificationProvider !== "noop",
    deviceCount: devices.length,
    record
  };
}

