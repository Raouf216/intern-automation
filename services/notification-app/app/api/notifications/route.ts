import { NextResponse } from "next/server";
import { listNotifications, notificationConfigStatus } from "@/lib/notifications";

export async function GET() {
  try {
    const notifications = await listNotifications(120);

    return NextResponse.json({
      ok: true,
      config: notificationConfigStatus(),
      notifications,
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown error";

    return NextResponse.json({ ok: false, error: message }, { status: 500 });
  }
}
