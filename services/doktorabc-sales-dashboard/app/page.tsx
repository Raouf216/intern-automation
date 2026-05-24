import { cookies } from "next/headers";
import { redirect } from "next/navigation";
import { SalesDashboard } from "./sales-dashboard";
import { salesSessionCookie, verifySessionToken } from "../lib/auth";
import { listSalesRows, type SalesRow } from "../lib/sales-data";

export const dynamic = "force-dynamic";

export default async function Home() {
  const cookieStore = await cookies();

  if (!verifySessionToken(cookieStore.get(salesSessionCookie)?.value)) {
    redirect("/login");
  }

  let rows: SalesRow[] = [];
  let loadError: string | null = null;

  try {
    rows = await listSalesRows();
  } catch (error) {
    loadError = error instanceof Error ? error.message : "Unbekannter Fehler beim Laden der Verkaufsdaten.";
  }

  return <SalesDashboard initialRows={rows} loadError={loadError} />;
}
