import { NextResponse } from "next/server";

export const dynamic = "force-dynamic";

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

function storageHeaders() {
  const serviceRoleKey = requiredEnv("SUPABASE_SERVICE_ROLE_KEY");

  return {
    apikey: serviceRoleKey,
    Authorization: `Bearer ${serviceRoleKey}`,
  };
}

function configuredScreenshotBucket() {
  return (process.env.SUPABASE_STOCK_SCREENSHOTS_BUCKET || "abrechnung-stock-screenshots").trim() || "abrechnung-stock-screenshots";
}

function storageObjectUrl(bucket: string, objectPath: string) {
  const encodedPath = objectPath.split("/").map(encodeURIComponent).join("/");
  return `${supabaseUrl()}/storage/v1/object/authenticated/${encodeURIComponent(bucket)}/${encodedPath}`;
}

export async function GET(_request: Request, context: { params: Promise<{ path?: string[] }> }) {
  try {
    const params = await context.params;
    const parts = params.path || [];
    const [bucket, ...objectParts] = parts;
    const objectPath = objectParts.join("/");

    if (!bucket || !objectPath) {
      return NextResponse.json({ ok: false, error: "missing_screenshot_path" }, { status: 400 });
    }

    if (bucket !== configuredScreenshotBucket()) {
      return NextResponse.json({ ok: false, error: "invalid_screenshot_bucket" }, { status: 403 });
    }

    const storageResponse = await fetch(storageObjectUrl(bucket, objectPath), {
      headers: storageHeaders(),
      cache: "no-store",
    });

    if (!storageResponse.ok) {
      const detail = await storageResponse.text();
      return NextResponse.json({ ok: false, error: detail || "screenshot_not_found" }, { status: storageResponse.status });
    }

    const contentType = storageResponse.headers.get("content-type") || "image/png";
    const bytes = await storageResponse.arrayBuffer();

    return new NextResponse(bytes, {
      status: 200,
      headers: {
        "Content-Type": contentType,
        "Cache-Control": "private, max-age=60",
      },
    });
  } catch (error) {
    return NextResponse.json(
      {
        ok: false,
        error: error instanceof Error ? error.message : "Unknown screenshot error.",
      },
      { status: 500 }
    );
  }
}
