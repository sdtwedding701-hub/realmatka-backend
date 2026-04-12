import {
  addSupportChatMessage,
  getSupportConversationBundleForUser,
  getSupportConversationDetailsForAdmin,
  listSupportConversations,
  markSupportMessagesReadByAdmin,
  markSupportMessagesReadByUser,
  requireUserByToken
} from "../db.mjs";
import { corsPreflight, fail, getJsonBody, getSessionToken, ok, unauthorized } from "../http.mjs";

export function options(request) {
  return corsPreflight(request);
}

async function requireAdmin(request) {
  const user = await requireUserByToken(getSessionToken(request));
  if (!user) {
    return { user: null, response: unauthorized(request) };
  }
  if (user.role !== "admin") {
    return { user: null, response: fail("Admin access required", 403, request) };
  }
  return { user, response: null };
}

export async function userConversation(request) {
  const user = await requireUserByToken(getSessionToken(request));
  if (!user) {
    return unauthorized(request);
  }

  const bundle = await getSupportConversationBundleForUser(user.id);
  await markSupportMessagesReadByUser(bundle.conversation.id);
  const refreshed = await getSupportConversationBundleForUser(user.id);

  return ok(refreshed, request);
}

export async function userSend(request) {
  const user = await requireUserByToken(getSessionToken(request));
  if (!user) {
    return unauthorized(request);
  }

  const body = await getJsonBody(request);
  const text = String(body.text ?? "").trim();
  if (!text) {
    return fail("Message text is required", 400, request);
  }
  if (text.length > 1000) {
    return fail("Message is too long", 400, request);
  }

  const bundle = await getSupportConversationBundleForUser(user.id);
  const message = await addSupportChatMessage({
    conversationId: bundle.conversation.id,
    senderRole: "user",
    senderUserId: user.id,
    text,
    readByUser: true,
    readByAdmin: false
  });

  return ok({ conversationId: bundle.conversation.id, message }, request);
}

export async function adminConversations(request) {
  const admin = await requireAdmin(request);
  if (admin.response) return admin.response;

  return ok(await listSupportConversations(), request);
}

export async function adminMessages(request) {
  const admin = await requireAdmin(request);
  if (admin.response) return admin.response;

  const url = new URL(request.url);
  const conversationId = String(url.searchParams.get("conversationId") ?? "").trim();
  if (!conversationId) {
    return fail("conversationId is required", 400, request);
  }

  await markSupportMessagesReadByAdmin(conversationId);
  const details = await getSupportConversationDetailsForAdmin(conversationId);
  if (!details) {
    return fail("Conversation not found", 404, request);
  }

  return ok(details, request);
}

export async function adminSend(request) {
  const admin = await requireAdmin(request);
  if (admin.response) return admin.response;

  const body = await getJsonBody(request);
  const conversationId = String(body.conversationId ?? "").trim();
  const text = String(body.text ?? "").trim();
  if (!conversationId || !text) {
    return fail("conversationId and text are required", 400, request);
  }
  if (text.length > 1000) {
    return fail("Message is too long", 400, request);
  }

  const details = await getSupportConversationDetailsForAdmin(conversationId);
  if (!details) {
    return fail("Conversation not found", 404, request);
  }

  const message = await addSupportChatMessage({
    conversationId,
    senderRole: "support",
    senderUserId: admin.user.id,
    text,
    readByUser: false,
    readByAdmin: true
  });

  return ok({ conversationId, message }, request);
}
