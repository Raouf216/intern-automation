import { NextResponse } from "next/server";

export const dynamic = "force-dynamic";

type ReviewDecision = "verified" | "needs_review";
type ReviewIssue = "product" | "quantity" | "charge" | "expiry" | "price" | "other";

const reviewIssueLabels: Record<ReviewIssue, string> = {
  product: "Produkt",
  quantity: "Menge",
  charge: "Charge",
  expiry: "Ablaufdatum",
  price: "Preis",
  other: "Sonstiges",
};

const reviewIssueSet = new Set(Object.keys(reviewIssueLabels));

function requiredEnv(name: string) {
  const value = process.env[name];

  if (!value?.trim()) {
    throw new Error(`Missing required environment variable: ${name}`);
  }

  return value.trim().replace(/^["']|["']$/g, "");
}

function supabaseUrl() {
  const value = requiredEnv("SUPABASE_URL").replace(/\/+$/, "");

  try {
    return new URL(value).href.replace(/\/+$/, "");
  } catch {
    throw new Error("Invalid SUPABASE_URL. Use a full URL such as http://supabase-kong:8000.");
  }
}

function schemaName() {
  return (process.env.SUPABASE_ABRECHNUNG_SCHEMA || "private").trim() || "private";
}

function tableName() {
  return (process.env.SUPABASE_ABRECHNUNGEN_TABLE || "abrechnungen").trim() || "abrechnungen";
}

function supabaseHeaders() {
  const serviceRoleKey = requiredEnv("SUPABASE_SERVICE_ROLE_KEY");
  const schema = schemaName();

  return {
    apikey: serviceRoleKey,
    Authorization: `Bearer ${serviceRoleKey}`,
    "Accept-Profile": schema,
    "Content-Profile": schema,
    "Content-Type": "application/json",
  };
}

function restUrl() {
  return `${supabaseUrl()}/rest/v1/${encodeURIComponent(tableName())}`;
}

function stringFromPayload(payload: Record<string, unknown>, key: string) {
  const value = payload[key];

  return typeof value === "string" ? value.trim() : "";
}

function decisionFromPayload(payload: Record<string, unknown>): ReviewDecision | "" {
  const decision = stringFromPayload(payload, "decision");
  if (decision === "verified" || decision === "needs_review") return decision;
  return "";
}

function cleanText(value: string, maxLength: number) {
  return value.replace(/[\u0000-\u001f\u007f]/g, " ").replace(/\s+/g, " ").trim().slice(0, maxLength);
}

function issueTypesFromPayload(payload: Record<string, unknown>) {
  const issueTypes = payload.issueTypes;
  if (!Array.isArray(issueTypes)) return [];

  return Array.from(
    new Set(
      issueTypes
        .map((issueType) => (typeof issueType === "string" ? issueType.trim() : ""))
        .filter((issueType): issueType is ReviewIssue => reviewIssueSet.has(issueType))
    )
  );
}

function buildReviewNote(payload: Record<string, unknown>, decision: ReviewDecision) {
  if (decision === "verified") return "Geprüft: Ware stimmt mit der Abrechnung überein.";

  const issueTypes = issueTypesFromPayload(payload);
  if (!issueTypes.length) {
    throw new Error("Mindestens ein Abweichungsbereich muss ausgewählt werden.");
  }

  const positionLabel = cleanText(stringFromPayload(payload, "positionLabel"), 120) || "Gesamte Abrechnung";
  const detail = cleanText(stringFromPayload(payload, "detail"), 500);
  const issueText = issueTypes.map((issueType) => reviewIssueLabels[issueType]).join(", ");

  return [`Abweichung: ${positionLabel}`, `Bereich: ${issueText}`, detail ? `Hinweis: ${detail}` : ""].filter(Boolean).join(" | ");
}

export async function POST(request: Request) {
  try {
    const payload = (await request.json()) as Record<string, unknown>;
    const id = stringFromPayload(payload, "id");
    const decision = decisionFromPayload(payload);

    if (!id || !decision) {
      return NextResponse.json(
        {
          ok: false,
          error: "id and decision are required.",
        },
        { status: 400 }
      );
    }

    const reviewNote = buildReviewNote(payload, decision);

    const url = new URL(restUrl());
    url.searchParams.set("id", `eq.${id}`);

    const response = await fetch(url, {
      method: "PATCH",
      headers: {
        ...supabaseHeaders(),
        Prefer: "return=representation",
      },
      body: JSON.stringify({
        status: decision,
        review_note: reviewNote,
      }),
      cache: "no-store",
    });

    if (!response.ok) {
      throw new Error(`Supabase Abrechnung review update failed (${response.status}): ${await response.text()}`);
    }

    const rows = (await response.json()) as unknown[];

    if (rows.length !== 1) {
      throw new Error(rows.length === 0 ? "Abrechnung was not found." : "Abrechnung update returned more than one row.");
    }

    return NextResponse.json({
      ok: true,
      id,
      status: decision,
      reviewNote,
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown Abrechnung review error.";

    return NextResponse.json(
      {
        ok: false,
        error: message,
      },
      { status: 500 }
    );
  }
}
