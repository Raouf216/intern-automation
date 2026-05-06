import { NextResponse } from "next/server";

const webhookUrl =
  process.env.PRODUCT_SYNC_N8N_WEBHOOK_URL ||
  process.env.N8N_PRODUCT_SYNC_WEBHOOK_URL ||
  process.env.N8N_UPLOAD_WEBHOOK_URL ||
  "";

export async function POST(request: Request) {
  let payload: Record<string, unknown>;

  try {
    payload = (await request.json()) as Record<string, unknown>;
  } catch {
    return NextResponse.json({ ok: false, error: "invalid_json" }, { status: 400 });
  }

  if (!webhookUrl.trim()) {
    return NextResponse.json({ ok: true, skipped: true, reason: "webhook_not_configured" });
  }

  try {
    const response = await fetch(webhookUrl.trim(), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
      cache: "no-store",
    });

    if (!response.ok) {
      return NextResponse.json(
        {
          ok: false,
          error: `n8n_webhook_failed_${response.status}`,
          detail: await response.text(),
        },
        { status: 502 }
      );
    }

    return NextResponse.json({ ok: true });
  } catch (error) {
    return NextResponse.json(
      {
        ok: false,
        error: error instanceof Error ? error.message : "unknown_webhook_error",
      },
      { status: 502 }
    );
  }
}
