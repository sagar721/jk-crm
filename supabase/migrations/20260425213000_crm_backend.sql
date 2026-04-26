begin;

create extension if not exists pgcrypto;

create or replace function public.set_updated_at()
returns trigger
language plpgsql
as $$
begin
  new.updated_at = timezone('utc', now());
  return new;
end;
$$;

create table if not exists public.companies (
  id uuid primary key default gen_random_uuid(),
  name text not null,
  industry text,
  size text,
  website text,
  location text,
  status text not null default 'lead' check (status in ('lead', 'qualified', 'converted', 'inactive')),
  user_id uuid not null default auth.uid() references auth.users(id) on delete cascade,
  created_at timestamptz not null default timezone('utc', now()),
  updated_at timestamptz not null default timezone('utc', now()),
  constraint companies_id_user_id_key unique (id, user_id)
);

create table if not exists public.contacts (
  id uuid primary key default gen_random_uuid(),
  name text not null,
  email text not null,
  phone text,
  company_id uuid not null,
  user_id uuid not null default auth.uid() references auth.users(id) on delete cascade,
  status text not null default 'lead' check (status in ('lead', 'contacted', 'converted', 'inactive')),
  created_at timestamptz not null default timezone('utc', now()),
  updated_at timestamptz not null default timezone('utc', now()),
  constraint contacts_company_fk foreign key (company_id) references public.companies(id) on delete cascade,
  constraint contacts_company_user_fk foreign key (company_id, user_id) references public.companies(id, user_id) on delete cascade
);

create table if not exists public.orders (
  id uuid primary key default gen_random_uuid(),
  amount numeric(12, 2) not null check (amount >= 0),
  status text not null default 'pending' check (status in ('pending', 'processing', 'fulfilled', 'cancelled')),
  company_id uuid not null,
  user_id uuid not null default auth.uid() references auth.users(id) on delete cascade,
  created_at timestamptz not null default timezone('utc', now()),
  updated_at timestamptz not null default timezone('utc', now()),
  constraint orders_company_fk foreign key (company_id) references public.companies(id) on delete cascade,
  constraint orders_company_user_fk foreign key (company_id, user_id) references public.companies(id, user_id) on delete cascade
);

create unique index if not exists contacts_email_unique_idx on public.contacts (lower(email));
create index if not exists companies_user_id_idx on public.companies (user_id);
create index if not exists companies_user_status_idx on public.companies (user_id, status);
create index if not exists contacts_user_id_idx on public.contacts (user_id);
create index if not exists contacts_company_id_idx on public.contacts (company_id);
create index if not exists contacts_user_company_idx on public.contacts (user_id, company_id);
create index if not exists orders_user_id_idx on public.orders (user_id);
create index if not exists orders_company_id_idx on public.orders (company_id);
create index if not exists orders_user_company_idx on public.orders (user_id, company_id);
create index if not exists orders_user_status_idx on public.orders (user_id, status);

drop trigger if exists set_companies_updated_at on public.companies;
create trigger set_companies_updated_at
before update on public.companies
for each row
execute function public.set_updated_at();

drop trigger if exists set_contacts_updated_at on public.contacts;
create trigger set_contacts_updated_at
before update on public.contacts
for each row
execute function public.set_updated_at();

drop trigger if exists set_orders_updated_at on public.orders;
create trigger set_orders_updated_at
before update on public.orders
for each row
execute function public.set_updated_at();

alter table public.companies enable row level security;
alter table public.contacts enable row level security;
alter table public.orders enable row level security;

alter table public.companies force row level security;
alter table public.contacts force row level security;
alter table public.orders force row level security;

drop policy if exists "users_manage_own_companies" on public.companies;
create policy "users_manage_own_companies"
on public.companies
for all
using (auth.uid() = user_id)
with check (auth.uid() = user_id);

drop policy if exists "users_manage_own_contacts" on public.contacts;
create policy "users_manage_own_contacts"
on public.contacts
for all
using (auth.uid() = user_id)
with check (auth.uid() = user_id);

drop policy if exists "users_manage_own_orders" on public.orders;
create policy "users_manage_own_orders"
on public.orders
for all
using (auth.uid() = user_id)
with check (auth.uid() = user_id);

revoke all on public.companies from anon;
revoke all on public.contacts from anon;
revoke all on public.orders from anon;

grant select, insert, update, delete on public.companies to authenticated;
grant select, insert, update, delete on public.contacts to authenticated;
grant select, insert, update, delete on public.orders to authenticated;

commit;
