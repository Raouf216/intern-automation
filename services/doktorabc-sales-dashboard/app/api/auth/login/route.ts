import { NextResponse } from "next/server";
import { createSessionToken, isPasswordConfigured, isValidPassword, salesSessionCookie, sessionCookieOptions } from "../../../../lib/auth";

export async function POST(request: Request) {
  const payload = (await request.json().catch(() => null)) as { password?: string } | null;
  const password = payload?.password || "";

  if (!isPasswordConfigured()) {
    return NextResponse.json({ ok: false, error: "SALES_DASHBOARD_PASSWORD ist nicht gesetzt." }, { status: 500 });
  }

  if (!isValidPassword(password)) {
    return NextResponse.json({ ok: false, error: "Passwort ist falsch." }, { status: 401 });
  }

  const response = NextResponse.json({ ok: true });
  response.cookies.set(salesSessionCookie, createSessionToken(), sessionCookieOptions());
  return response;
}
