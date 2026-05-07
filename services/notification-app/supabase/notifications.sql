create table if not exists public.notifications (
  id uuid primary key default gen_random_uuid(),
  section text not null default 'upload',
  event text not null,
  status text not null,
  title text not null,
  message text not null,
  filename text,
  upload_type text,
  bucket text,
  path text,
  size_bytes bigint,
  error text,
  source text not null default 'n8n',
  payload jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create index if not exists notifications_created_at_idx
  on public.notifications (created_at desc);

create index if not exists notifications_section_created_at_idx
  on public.notifications (section, created_at desc);

alter table public.notifications enable row level security;
