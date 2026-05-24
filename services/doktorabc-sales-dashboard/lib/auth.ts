import { createHmac, randomUUID, timingSafeEqual } from "node:crypto";

export const salesSessionCookie = "doktorabc_sales_session";

const sessionMaxAgeSeconds = 60 * 60 * 12;

function dashboardPassword() {
  return process.env.SALES_DASHBOARD_PASSWORD || "";
}

function sessionSecret() {
  return process.env.SALES_DASHBOARD_SESSION_SECRET || dashboardPassword();
}

function sign(value: string) {
  return createHmac("sha256", sessionSecret()).update(value).digest("base64url");
}

function safeEquals(left: string, right: string) {
  const leftBuffer = Buffer.from(left);
  const rightBuffer = Buffer.from(right);

  return leftBuffer.length === rightBuffer.length && timingSafeEqual(leftBuffer, rightBuffer);
}

export function isPasswordConfigured() {
  return Boolean(dashboardPassword());
}

export function isValidPassword(value: string) {
  const password = dashboardPassword();

  return Boolean(password) && safeEquals(value, password);
}

export function createSessionToken() {
  const payload = `${Date.now()}.${randomUUID()}`;
  return `${payload}.${sign(payload)}`;
}

export function verifySessionToken(token: string | undefined) {
  if (!token || !sessionSecret()) {
    return false;
  }

  const parts = token.split(".");
  if (parts.length !== 3) {
    return false;
  }

  const payload = `${parts[0]}.${parts[1]}`;
  const createdAt = Number(parts[0]);

  if (!Number.isFinite(createdAt) || Date.now() - createdAt > sessionMaxAgeSeconds * 1000) {
    return false;
  }

  return safeEquals(parts[2], sign(payload));
}

export function sessionCookieOptions() {
  return {
    httpOnly: true,
    maxAge: sessionMaxAgeSeconds,
    path: "/",
    sameSite: "lax" as const,
    secure: process.env.NODE_ENV === "production",
  };
}
