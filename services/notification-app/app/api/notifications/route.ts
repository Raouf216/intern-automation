import { NextResponse } from "next/server";
import { listNotifications, notificationConfigStatus } from "@/lib/notifications";

export async function GET(request: Request) {
  try {
    const { searchParams } = new URL(request.url);
    const realtimeStart = searchParams.get("realtime_start");
    const realtimeEnd = searchParams.get("realtime_end");
    const notifications = await listNotifications({
      limit: 120,
      realtimeStart,
      realtimeEnd,
      realtimeLimit: 5000,
    });

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
