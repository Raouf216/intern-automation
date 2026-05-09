import { NextResponse } from "next/server";
import { listVerificationRuns, storeVerificationRun } from "../../../lib/verification-store";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

export async function GET(request: Request) {
  const url = new URL(request.url);
  const limit = Number(url.searchParams.get("limit") || 160);

  try {
    const runs = await listVerificationRuns(Number.isFinite(limit) ? limit : 160);
    return NextResponse.json({ ok: true, runs });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown error";
    return NextResponse.json({ ok: false, error: message }, { status: 500 });
  }
}

export async function POST(request: Request) {
  const expectedToken = process.env.ABRECHNUNG_VERIFICATION_INGEST_TOKEN || "";

  if (expectedToken) {
    const authorization = request.headers.get("authorization") || "";
    const bearerToken = authorization.toLowerCase().startsWith("bearer ")
      ? authorization.slice("bearer ".length).trim()
      : "";
    const headerToken = request.headers.get("x-ingest-token") || "";

    if (bearerToken !== expectedToken && headerToken !== expectedToken) {
      return NextResponse.json({ ok: false, error: "unauthorized" }, { status: 401 });
    }
  }

  let payload: unknown;

  try {
    payload = (await request.json()) as Record<string, unknown>;
  } catch {
    return NextResponse.json({ ok: false, error: "invalid_json" }, { status: 400 });
  }

  try {
    const run = await storeVerificationRun(payload);
    return NextResponse.json({ ok: true, run }, { status: 201 });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown error";
    return NextResponse.json({ ok: false, error: message }, { status: 500 });
  }
}
