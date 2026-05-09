# Product Sync Signal UI

Next.js TypeScript control surface for triggering the DoktorABC product sync bot, the End-of-Day orders/export bot, and the Pickup Ready bot.

The bot buttons still call the configured bot endpoints from the browser. The manual Self Pickup pickup marker uses an internal Next.js API route so the Supabase service-role key stays server-side.

## Environment

```env
PRODUCT_SYNC_SIGNAL_UI_PORT=8040
TZ=Europe/Berlin
NEXT_PUBLIC_PRODUCT_SYNC_ENDPOINT=http://178.104.144.30:8020/jobs/product-prices
NEXT_PUBLIC_EOD_ORDERS_ENDPOINT=http://178.104.144.30:8021/jobs/end-of-day/orders/sync
NEXT_PUBLIC_PRODUCT_SYNC_PASSWORD=change-me
PICKUP_DONE_BOT_ENDPOINT=http://178.104.144.30:8023/jobs/pickup-ready/orders/mark-picked
PICKUP_DONE_DRY_RUN=true
SUPABASE_URL=http://supabase-kong:8000
SUPABASE_SERVICE_ROLE_KEY=...
SUPABASE_EOD_ORDERS_SCHEMA=private
SUPABASE_EOD_ORDERS_TABLE=doktorabc_eod_bot_orders
```

When `PICKUP_DONE_BOT_ENDPOINT` is configured, the Self Pickup button first asks the pickup action bot to find the DoktorABC order card and test/click the `Self pickup done` button. With `PICKUP_DONE_DRY_RUN=true`, Supabase is not marked as picked; the UI only reports whether the button would be clickable. Set `PICKUP_DONE_DRY_RUN=false` after the dry-run result is trusted.

## Local Start

```powershell
cd C:\Work\Apotheke\intern-management\services\product-sync-signal-ui
npm install
npm run dev -- -p 8040
```

Open:

```txt
http://localhost:8040
```

## Docker Start

```powershell
cd C:\Work\Apotheke\intern-management\services\product-sync-signal-ui
docker compose up --build
```

Open:

```txt
http://localhost:8040
```

## Production Note

For a more secure production version, move the bot endpoint calls behind server-side routes too. That would keep every backend endpoint out of the browser.
