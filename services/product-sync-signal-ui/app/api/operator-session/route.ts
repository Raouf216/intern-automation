import { createHmac, timingSafeEqual } from "crypto";
import { NextResponse } from "next/server";

export const dynamic = "force-dynamic";

const SESSION_COOKIE_NAME = "product_sync_operator_session";
const SESSION_MAX_AGE_MS = 12 * 60 * 60 * 1000;

function operatorPassword() {
  return (process.env.NEXT_PUBLIC_PRODUCT_SYNC_PASSWORD || "").trim();
}

function sessionSignature(issuedAt: string) {
  return createHmac("sha256", operatorPassword())
    .update(`product-sync-operator:${issuedAt}`)
    .digest("base64url");
}

function createSessionToken() {
  const issuedAt = String(Date.now());
  return `${issuedAt}.${sessionSignature(issuedAt)}`;
}

function cookieValue(request: Request, name: string) {
  const cookies = request.headers.get("cookie") || "";

  return cookies
    .split(";")
    .map((cookie) => cookie.trim())
    .map((cookie) => {
      const separatorIndex = cookie.indexOf("=");
      return separatorIndex === -1
        ? [cookie, ""]
        : [cookie.slice(0, separatorIndex), decodeURIComponent(cookie.slice(separatorIndex + 1))];
    })
    .find(([cookieName]) => cookieName === name)?.[1] || "";
}

function isValidSessionToken(value: string) {
  if (!value || !operatorPassword()) return false;

  const [issuedAt, receivedSignature] = value.split(".");
  const issuedAtMs = Number(issuedAt);

  if (!issuedAt || !receivedSignature || !Number.isFinite(issuedAtMs)) return false;
  if (Date.now() - issuedAtMs > SESSION_MAX_AGE_MS) return false;

  const expectedSignature = sessionSignature(issuedAt);
  const received = Buffer.from(receivedSignature);
  const expected = Buffer.from(expectedSignature);

  return received.length === expected.length && timingSafeEqual(received, expected);
}

function setSessionCookie(response: NextResponse, request: Request) {
  response.cookies.set(SESSION_COOKIE_NAME, createSessionToken(), {
    httpOnly: true,
    maxAge: Math.floor(SESSION_MAX_AGE_MS / 1000),
    path: "/",
    sameSite: "lax",
    secure: new URL(request.url).protocol === "https:",
  });
}

function validatePassword(receivedPassword: string) {
  const expectedPassword = operatorPassword();

  if (!expectedPassword) {
    return "operator_password_not_configured";
  }

  return receivedPassword === expectedPassword ? null : "operator_password_invalid";
}

export async function GET(request: Request) {
  if (!operatorPassword()) {
    return NextResponse.json({ ok: false, error: "operator_password_not_configured" }, { status: 500 });
  }

  if (!isValidSessionToken(cookieValue(request, SESSION_COOKIE_NAME))) {
    return NextResponse.json({ ok: false, error: "operator_session_invalid" }, { status: 401 });
  }

  return NextResponse.json({ ok: true });
}

export async function POST(request: Request) {
  let payload: Record<string, unknown>;

  try {
    payload = (await request.json()) as Record<string, unknown>;
  } catch {
    return NextResponse.json({ ok: false, error: "invalid_json" }, { status: 400 });
  }

  const receivedPassword = String(payload.operator_password || payload.operatorPassword || "");
  const passwordError = validatePassword(receivedPassword);

  if (passwordError) {
    return NextResponse.json(
      { ok: false, error: passwordError },
      { status: passwordError.includes("not_configured") ? 500 : 401 }
    );
  }

  const response = NextResponse.json({ ok: true });
  setSessionCookie(response, request);

  return response;
}
