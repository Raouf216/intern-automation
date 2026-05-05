import { NotificationDashboard } from "./notification-dashboard";
import { listNotifications, notificationConfigStatus, type StoredNotification } from "@/lib/notifications";

export const dynamic = "force-dynamic";

export default async function Home() {
  const config = notificationConfigStatus();
  let notifications: StoredNotification[] = [];
  let loadError: string | null = null;

  try {
    notifications = await listNotifications(120);
  } catch (error) {
    loadError = error instanceof Error ? error.message : "Unbekannter Fehler";
  }

  return <NotificationDashboard config={config} initialError={loadError} initialNotifications={notifications} />;
}
