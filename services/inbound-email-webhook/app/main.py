import asyncio
import base64
import json
import logging
import os
import signal
import threading
import uuid
from datetime import datetime, timezone
from email import policy
from email.parser import BytesParser
from email.utils import getaddresses, parsedate_to_datetime
from html import escape
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import httpx
from aiosmtpd.controller import Controller
from aiosmtpd.smtp import Envelope, Session


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("inbound-email-webhook")


def env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def env_list(name: str) -> set[str]:
    value = os.getenv(name, "")
    return {item.strip().lower() for item in value.split(",") if item.strip()}


WEBHOOK_URL = os.getenv("WEBHOOK_URL", "").strip()
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "").strip()
WEBHOOK_BEARER_TOKEN = os.getenv("WEBHOOK_BEARER_TOKEN", "").strip()
SMTP_HOST = os.getenv("SMTP_HOST", "0.0.0.0").strip()
SMTP_PORT = int(os.getenv("SMTP_PORT", "2525"))
SMTP_HOSTNAME = os.getenv("SMTP_HOSTNAME", "mail.local").strip()
HTTP_PORT = int(os.getenv("PORT", "8000"))
MAX_MESSAGE_BYTES = int(os.getenv("MAX_MESSAGE_BYTES", str(10 * 1024 * 1024)))
INCLUDE_ATTACHMENT_CONTENT = env_bool("INCLUDE_ATTACHMENT_CONTENT", True)
ACCEPTED_RECIPIENTS = env_list("ACCEPTED_RECIPIENTS")
ACCEPTED_DOMAINS = env_list("ACCEPTED_DOMAINS")
MAX_RECENT_EVENTS = int(os.getenv("MAX_RECENT_EVENTS", "50"))
EVENT_HISTORY_PATH = os.getenv("EVENT_HISTORY_PATH", "/app/data/events.jsonl").strip()
MAX_HISTORY_FILE_BYTES = int(os.getenv("MAX_HISTORY_FILE_BYTES", str(5 * 1024 * 1024)))
recent_events: list[dict[str, Any]] = []
recent_events_lock = threading.Lock()


def remember_event(event: dict[str, Any]) -> None:
    record = {"event_id": event.get("event_id") or str(uuid.uuid4()), "time": datetime.now(timezone.utc).isoformat(), **event}
    with recent_events_lock:
        recent_events.insert(0, record)
        del recent_events[MAX_RECENT_EVENTS:]
    append_event_history(record)


def append_event_history(event: dict[str, Any]) -> None:
    if not EVENT_HISTORY_PATH:
        return
    try:
        directory = os.path.dirname(EVENT_HISTORY_PATH)
        if directory:
            os.makedirs(directory, exist_ok=True)
        if os.path.exists(EVENT_HISTORY_PATH) and os.path.getsize(EVENT_HISTORY_PATH) > MAX_HISTORY_FILE_BYTES:
            rotated_path = f"{EVENT_HISTORY_PATH}.1"
            if os.path.exists(rotated_path):
                os.remove(rotated_path)
            os.replace(EVENT_HISTORY_PATH, rotated_path)
        with open(EVENT_HISTORY_PATH, "a", encoding="utf-8") as file:
            file.write(json.dumps(event, ensure_ascii=False, default=str) + "\n")
    except Exception:
        logger.exception("Failed to write event history")


def load_event_history() -> None:
    if not EVENT_HISTORY_PATH or not os.path.exists(EVENT_HISTORY_PATH):
        return
    try:
        with open(EVENT_HISTORY_PATH, "r", encoding="utf-8") as file:
            lines = file.readlines()[-MAX_RECENT_EVENTS:]
        loaded = [json.loads(line) for line in lines if line.strip()]
        with recent_events_lock:
            recent_events[:] = list(reversed(loaded))
    except Exception:
        logger.exception("Failed to load event history")


def parse_date(value: str | None) -> str | None:
    if not value:
        return None
    try:
        parsed = parsedate_to_datetime(value)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.isoformat()
    except (TypeError, ValueError):
        return value


def decode_text_part(part: Any) -> str:
    try:
        content = part.get_content()
        return content if isinstance(content, str) else str(content)
    except Exception:
        payload = part.get_payload(decode=True) or b""
        charset = part.get_content_charset() or "utf-8"
        return payload.decode(charset, errors="replace")


def collect_addresses(value: str | None) -> list[dict[str, str]]:
    return [{"name": name, "email": email} for name, email in getaddresses([value or ""]) if email]


def parse_message(raw: bytes, mail_from: str, rcpt_tos: list[str]) -> dict[str, Any]:
    message = BytesParser(policy=policy.default).parsebytes(raw)
    text_parts: list[str] = []
    html_parts: list[str] = []
    attachments: list[dict[str, Any]] = []

    for part in message.walk():
        if part.is_multipart():
            continue

        content_type = part.get_content_type()
        disposition = (part.get_content_disposition() or "").lower()
        filename = part.get_filename()

        if disposition == "attachment" or filename:
            content = part.get_payload(decode=True) or b""
            attachment: dict[str, Any] = {
                "filename": filename or "attachment",
                "content_type": content_type,
                "size": len(content),
            }
            if INCLUDE_ATTACHMENT_CONTENT:
                attachment["content_base64"] = base64.b64encode(content).decode("ascii")
            attachments.append(attachment)
            continue

        if content_type == "text/plain":
            text_parts.append(decode_text_part(part))
        elif content_type == "text/html":
            html_parts.append(decode_text_part(part))

    headers = {key: value for key, value in message.items()}
    message_id = message.get("Message-ID") or message.get("Message-Id") or ""

    return {
        "source": "inbound-email-webhook",
        "message_id": message_id,
        "source_key": message_id or f"{mail_from}|{','.join(rcpt_tos)}|{message.get('Date', '')}|{message.get('Subject', '')}",
        "smtp_from": mail_from,
        "smtp_to": rcpt_tos,
        "from": message.get("From", mail_from),
        "from_addresses": collect_addresses(message.get("From")),
        "to": message.get("To") or ", ".join(rcpt_tos),
        "to_addresses": collect_addresses(message.get("To")) or [{"name": "", "email": email} for email in rcpt_tos],
        "cc": message.get("Cc", ""),
        "subject": message.get("Subject", ""),
        "date": parse_date(message.get("Date")),
        "received_at": datetime.now(timezone.utc).isoformat(),
        "text_body": "\n".join(part.strip() for part in text_parts if part.strip()),
        "html_body": "\n".join(part.strip() for part in html_parts if part.strip()),
        "headers": headers,
        "attachments": attachments,
        "attachments_count": len(attachments),
        "raw_size": len(raw),
    }


def recipient_allowed(recipients: list[str]) -> bool:
    if not ACCEPTED_RECIPIENTS and not ACCEPTED_DOMAINS:
        return True

    for recipient in recipients:
        clean = recipient.lower().strip()
        domain = clean.rsplit("@", 1)[-1] if "@" in clean else ""
        if clean in ACCEPTED_RECIPIENTS or domain in ACCEPTED_DOMAINS:
            return True

    return False


async def deliver_webhook(payload: dict[str, Any]) -> None:
    if not WEBHOOK_URL:
        raise RuntimeError("WEBHOOK_URL is not configured")

    headers = {"Content-Type": "application/json"}
    if WEBHOOK_SECRET:
        headers["X-Inbound-Email-Secret"] = WEBHOOK_SECRET
    if WEBHOOK_BEARER_TOKEN:
        headers["Authorization"] = f"Bearer {WEBHOOK_BEARER_TOKEN}"

    async with httpx.AsyncClient(timeout=30) as client:
        response = await client.post(WEBHOOK_URL, json=payload, headers=headers)
        response.raise_for_status()


def safe_url_host(url: str) -> str:
    try:
        parsed = httpx.URL(url)
        return parsed.host or ""
    except Exception:
        return ""


def read_history_tail(limit: int = 500) -> list[dict[str, Any]]:
    if not EVENT_HISTORY_PATH or not os.path.exists(EVENT_HISTORY_PATH):
        return []
    try:
        with open(EVENT_HISTORY_PATH, "r", encoding="utf-8") as file:
            lines = file.readlines()[-limit:]
        return [json.loads(line) for line in lines if line.strip()]
    except Exception:
        logger.exception("Failed to read event history")
        return []


class InboundHandler:
    async def handle_DATA(self, server: Any, session: Session, envelope: Envelope) -> str:
        raw = envelope.original_content or envelope.content or b""
        if isinstance(raw, str):
            raw = raw.encode("utf-8", errors="replace")

        mail_from = envelope.mail_from or ""
        rcpt_tos = list(envelope.rcpt_tos or [])

        if len(raw) > MAX_MESSAGE_BYTES:
            logger.warning("Rejected oversized email from=%s size=%s", mail_from, len(raw))
            remember_event({"status": "rejected", "reason": "oversized", "from": mail_from, "to": rcpt_tos, "size": len(raw)})
            return "552 Message size exceeds configured limit"

        if not recipient_allowed(rcpt_tos):
            logger.warning("Rejected email for recipient outside allowlist from=%s to=%s", mail_from, rcpt_tos)
            remember_event({"status": "rejected", "reason": "recipient_not_allowed", "from": mail_from, "to": rcpt_tos})
            return "550 Recipient not accepted here"

        try:
            payload = parse_message(raw, mail_from, rcpt_tos)
            event_id = str(uuid.uuid4())
            await deliver_webhook(payload)
            remember_event(
                {
                    "event_id": event_id,
                    "status": "delivered",
                    "message_id": payload.get("message_id"),
                    "source_key": payload.get("source_key"),
                    "from": payload.get("from"),
                    "to": rcpt_tos,
                    "subject": payload.get("subject"),
                    "attachments": payload.get("attachments_count"),
                    "attachment_names": [attachment.get("filename") for attachment in payload.get("attachments", [])],
                    "size": len(raw),
                    "webhook_url_host": safe_url_host(WEBHOOK_URL),
                }
            )
            logger.info(
                "Delivered email webhook message_id=%s from=%s to=%s subject=%s attachments=%s",
                payload.get("message_id") or "(missing)",
                payload.get("from"),
                rcpt_tos,
                payload.get("subject"),
                payload.get("attachments_count"),
            )
            return "250 Message accepted"
        except httpx.HTTPStatusError as error:
            logger.exception("Webhook returned non-success status: %s", error.response.status_code)
            remember_event(
                {
                    "status": "webhook_error",
                    "reason": f"http_{error.response.status_code}",
                    "response_preview": error.response.text[:500],
                    "from": mail_from,
                    "to": rcpt_tos,
                    "size": len(raw),
                    "webhook_url_host": safe_url_host(WEBHOOK_URL),
                }
            )
            return "451 Webhook delivery failed, try again later"
        except Exception:
            logger.exception("Failed to parse or deliver inbound email")
            remember_event({"status": "error", "reason": "processing_failed", "from": mail_from, "to": rcpt_tos, "size": len(raw)})
            return "451 Processing failed, try again later"


class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        if self.path == "/health":
            body = json.dumps({"status": "ok"}).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        if self.path == "/events.json":
            with recent_events_lock:
                body = json.dumps(
                    {
                        "status": "ok",
                        "smtp_port": SMTP_PORT,
                        "webhook_configured": bool(WEBHOOK_URL),
                        "events": recent_events,
                    },
                    ensure_ascii=False,
                    default=str,
                ).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        if self.path == "/history.json":
            body = json.dumps(
                {
                    "status": "ok",
                    "history_path": EVENT_HISTORY_PATH,
                    "events": list(reversed(read_history_tail(500))),
                },
                ensure_ascii=False,
                default=str,
            ).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        if self.path == "/history.ndjson":
            body = "".join(json.dumps(event, ensure_ascii=False, default=str) + "\n" for event in read_history_tail(1000)).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/x-ndjson")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        if self.path not in {"/", "/ui"}:
            self.send_response(404)
            self.end_headers()
            return

        body = render_status_page().encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: Any) -> None:
        return


def render_status_page() -> str:
    with recent_events_lock:
        events = list(recent_events)

    rows = []
    for event in events:
        event_id = escape(str(event.get("event_id", "")))
        status = escape(str(event.get("status", "")))
        subject = escape(str(event.get("subject") or event.get("reason") or ""))
        sender = escape(str(event.get("from") or ""))
        recipients = escape(", ".join(event.get("to") or []))
        attachments = escape(str(event.get("attachments", "")))
        attachment_names = escape(", ".join(str(name) for name in event.get("attachment_names", []) if name))
        size = escape(str(event.get("size", "")))
        time = escape(str(event.get("time", "")))
        message_id = escape(str(event.get("message_id") or ""))
        source_key = escape(str(event.get("source_key") or ""))
        webhook_host = escape(str(event.get("webhook_url_host") or ""))
        response_preview = escape(str(event.get("response_preview") or ""))
        rows.append(
            f"<article class='event {status}'>"
            f"<div><strong>{status}</strong><span>{time}</span></div>"
            f"<h2>{subject or 'No subject'}</h2>"
            f"<p>{sender} -> {recipients}</p>"
            f"<dl>"
            f"<dt>Event</dt><dd>{event_id}</dd>"
            f"<dt>Message-ID</dt><dd>{message_id or '-'}</dd>"
            f"<dt>Source key</dt><dd>{source_key or '-'}</dd>"
            f"<dt>Webhook</dt><dd>{webhook_host or '-'}</dd>"
            f"<dt>Files</dt><dd>{attachment_names or '-'}</dd>"
            f"</dl>"
            f"{f'<pre>{response_preview}</pre>' if response_preview else ''}"
            f"<footer><span>Attachments: {attachments or '0'}</span><span>Bytes: {size or '0'}</span></footer>"
            "</article>"
        )

    events_html = "\n".join(rows) if rows else "<div class='empty'>No emails received yet.</div>"
    webhook_status = "configured" if WEBHOOK_URL else "missing"
    accepted = ", ".join(sorted(ACCEPTED_RECIPIENTS or ACCEPTED_DOMAINS)) or "all recipients"

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta http-equiv="refresh" content="20">
  <title>Inbound Email Webhook</title>
  <style>
    :root {{
      color-scheme: light;
      --bg: #f4f6f7;
      --surface: #ffffff;
      --ink: #172026;
      --muted: #667985;
      --line: #d8e1e6;
      --green: #057a55;
      --red: #b42331;
      --blue: #086f9d;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      color: var(--ink);
      background: var(--bg);
      font-family: Arial, Helvetica, sans-serif;
    }}
    main {{
      width: min(1060px, calc(100% - 28px));
      margin: 0 auto;
      padding: 22px 0 34px;
    }}
    header {{
      display: flex;
      justify-content: space-between;
      gap: 16px;
      border-bottom: 1px solid var(--line);
      padding-bottom: 18px;
    }}
    h1 {{
      margin: 0;
      font-size: 30px;
      line-height: 1.1;
    }}
    .meta {{
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      align-items: flex-start;
      justify-content: flex-end;
    }}
    .pill {{
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 8px 10px;
      color: var(--muted);
      background: var(--surface);
      font-size: 13px;
      font-weight: 800;
    }}
    a.pill {{ text-decoration: none; color: var(--blue); }}
    .feed {{
      display: grid;
      gap: 10px;
      margin-top: 16px;
    }}
    .event, .empty {{
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 15px;
      background: var(--surface);
    }}
    .event div, .event footer {{
      display: flex;
      justify-content: space-between;
      gap: 12px;
      color: var(--muted);
      font-size: 12px;
      font-weight: 800;
    }}
    .event strong {{ color: var(--blue); text-transform: uppercase; }}
    .event.delivered strong {{ color: var(--green); }}
    .event.rejected strong, .event.error strong, .event.webhook_error strong {{ color: var(--red); }}
    h2 {{
      margin: 10px 0 7px;
      font-size: 17px;
      line-height: 1.25;
    }}
    p {{
      margin: 0 0 12px;
      color: var(--muted);
      overflow-wrap: anywhere;
    }}
    dl {{
      display: grid;
      grid-template-columns: 110px minmax(0, 1fr);
      gap: 6px 10px;
      margin: 0 0 12px;
      font-size: 12px;
    }}
    dt {{
      color: var(--muted);
      font-weight: 800;
    }}
    dd {{
      min-width: 0;
      margin: 0;
      overflow-wrap: anywhere;
    }}
    pre {{
      max-height: 160px;
      overflow: auto;
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 10px;
      background: #f8fafb;
      white-space: pre-wrap;
      font-size: 12px;
    }}
    .empty {{
      min-height: 120px;
      display: grid;
      place-items: center;
      color: var(--muted);
      font-weight: 800;
    }}
    @media (max-width: 720px) {{
      header {{ flex-direction: column; }}
      .meta {{ justify-content: flex-start; }}
      .event div, .event footer {{ flex-direction: column; }}
    }}
  </style>
</head>
<body>
  <main>
    <header>
      <div>
        <h1>Inbound Email Webhook</h1>
      </div>
      <div class="meta">
        <span class="pill">SMTP :{SMTP_PORT}</span>
        <span class="pill">Webhook {escape(webhook_status)}</span>
        <span class="pill">{escape(accepted)}</span>
        <a class="pill" href="/events.json">events.json</a>
        <a class="pill" href="/history.ndjson">history.ndjson</a>
      </div>
    </header>
    <section class="feed" aria-label="Recent email events">
      {events_html}
    </section>
  </main>
</body>
</html>"""


def start_health_server() -> ThreadingHTTPServer:
    server = ThreadingHTTPServer(("0.0.0.0", HTTP_PORT), HealthHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    logger.info("Health server listening on :%s", HTTP_PORT)
    return server


async def main() -> None:
    if not WEBHOOK_URL:
        logger.warning("WEBHOOK_URL is empty; incoming SMTP messages will be temporarily rejected")

    load_event_history()
    health_server = start_health_server()
    controller = Controller(
        InboundHandler(),
        hostname=SMTP_HOST,
        port=SMTP_PORT,
        server_hostname=SMTP_HOSTNAME,
        decode_data=False,
    )
    controller.start()
    logger.info("SMTP server listening on %s:%s", SMTP_HOST, SMTP_PORT)

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            signal.signal(sig, lambda _signal, _frame: loop.call_soon_threadsafe(stop_event.set))

    try:
        await stop_event.wait()
    finally:
        logger.info("Shutting down")
        controller.stop()
        health_server.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
