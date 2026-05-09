import { AbrechnungVerificationDashboard } from "./verification-dashboard";
import { listVerificationRuns, type StoredVerificationRun } from "../lib/verification-store";

export const dynamic = "force-dynamic";

export default async function Home() {
  let runs: StoredVerificationRun[] = [];
  let loadError: string | null = null;

  try {
    runs = await listVerificationRuns(160);
  } catch (error) {
    loadError = error instanceof Error ? error.message : "Unbekannter Fehler";
  }

  return <AbrechnungVerificationDashboard initialError={loadError} initialRuns={runs} />;
}
