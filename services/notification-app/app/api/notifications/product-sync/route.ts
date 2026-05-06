import { NextResponse } from "next/server";
import { insertNotification, normalizeUploadNotification, type UploadWebhookPayload } from "@/lib/notifications";

export async function POST(request: Request) {
  let payload: UploadWebhookPayload;

  try {
    payload = (await request.json()) as UploadWebhookPayload;
  } catch {
    return NextResponse.json({ ok: false, error: "invalid_json" }, { status: 400 });
  }

  try {
    const notification = normalizeUploadNotification({
      ...payload,
      section: "doktorabc_sync",
      sync_type: payload.sync_type || "doktorabc_products",
    });
    const stored = await insertNotification(notification);

    return NextResponse.json({ ok: true, notification: stored }, { status: 201 });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown error";

    return NextResponse.json({ ok: false, error: message }, { status: 500 });
  }
}

export async function GET() {
  return NextResponse.json({
    ok: true,
    route: "/api/notifications/product-sync",
    accepts: ["doktorabc_sync_success", "doktorabc_sync_failure"],
  });
}
