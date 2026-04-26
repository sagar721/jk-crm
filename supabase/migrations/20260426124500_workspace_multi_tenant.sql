begin;

-- 1. Create Workspaces Table
create table if not exists public.workspaces (
    id uuid primary key default gen_random_uuid(),
    name text not null,
    created_at timestamptz not null default timezone('utc', now())
);

-- 2. Create Workspace Members Table
create table if not exists public.workspace_members (
    id uuid primary key default gen_random_uuid(),
    user_id uuid not null references auth.users(id) on delete cascade,
    workspace_id uuid not null references public.workspaces(id) on delete cascade,
    role text not null check (role in ('admin', 'member')),
    created_at timestamptz not null default timezone('utc', now()),
    unique(user_id, workspace_id)
);

-- 3. Create Invites Table
create table if not exists public.invites (
    id uuid primary key default gen_random_uuid(),
    email text not null,
    workspace_id uuid not null references public.workspaces(id) on delete cascade,
    role text not null check (role in ('admin', 'member')),
    status text not null default 'pending' check (status in ('pending', 'accepted')),
    created_at timestamptz not null default timezone('utc', now()),
    unique(email, workspace_id)
);

-- 4. Update Existing Tables
alter table public.companies add column if not exists workspace_id uuid references public.workspaces(id) on delete cascade;
alter table public.contacts add column if not exists workspace_id uuid references public.workspaces(id) on delete cascade;
alter table public.inquiries add column if not exists workspace_id uuid references public.workspaces(id) on delete cascade;
alter table public.quotations add column if not exists workspace_id uuid references public.workspaces(id) on delete cascade;
alter table public.orders add column if not exists workspace_id uuid references public.workspaces(id) on delete cascade;
alter table public.inquiry_items add column if not exists workspace_id uuid references public.workspaces(id) on delete cascade;
alter table public.quote_items add column if not exists workspace_id uuid references public.workspaces(id) on delete cascade;
alter table public.order_items add column if not exists workspace_id uuid references public.workspaces(id) on delete cascade;

-- Ensure row level security on workspaces
alter table public.workspaces enable row level security;
alter table public.workspace_members enable row level security;
alter table public.invites enable row level security;

alter table public.workspaces force row level security;
alter table public.workspace_members force row level security;
alter table public.invites force row level security;

-- Policies for Workspaces
create policy "users_read_own_workspaces" on public.workspaces
for select using (id in (select workspace_id from public.workspace_members where user_id = auth.uid()));

create policy "users_manage_workspace_members" on public.workspace_members
for all using (user_id = auth.uid() or workspace_id in (select workspace_id from public.workspace_members where user_id = auth.uid() and role = 'admin'));

create policy "admins_manage_invites" on public.invites
for all using (workspace_id in (select workspace_id from public.workspace_members where user_id = auth.uid() and role = 'admin'));

-- Update Policies for Existing Tables to use Workspace
drop policy if exists "users_manage_own_companies" on public.companies;
create policy "workspace_members_manage_companies" on public.companies
for all using (workspace_id in (select workspace_id from public.workspace_members where user_id = auth.uid()));

drop policy if exists "users_manage_own_contacts" on public.contacts;
create policy "workspace_members_manage_contacts" on public.contacts
for all using (workspace_id in (select workspace_id from public.workspace_members where user_id = auth.uid()));

drop policy if exists "users_manage_own_inquiries" on public.inquiries;
create policy "workspace_members_manage_inquiries" on public.inquiries
for all using (workspace_id in (select workspace_id from public.workspace_members where user_id = auth.uid()));

drop policy if exists "users_manage_own_quotations" on public.quotations;
create policy "workspace_members_manage_quotations" on public.quotations
for all using (workspace_id in (select workspace_id from public.workspace_members where user_id = auth.uid()));

drop policy if exists "users_manage_own_orders" on public.orders;
create policy "workspace_members_manage_orders" on public.orders
for all using (workspace_id in (select workspace_id from public.workspace_members where user_id = auth.uid()));

commit;
