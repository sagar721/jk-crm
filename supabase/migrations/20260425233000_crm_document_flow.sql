begin;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'contacts_id_user_id_key'
  ) then
    alter table public.contacts
      add constraint contacts_id_user_id_key unique (id, user_id);
  end if;
end;
$$;

create table if not exists public.inquiries (
  id uuid primary key default gen_random_uuid(),
  company_id uuid not null,
  contact_id uuid,
  status text not null default 'lead' check (status in ('lead', 'new', 'in_review', 'quoted', 'negotiation', 'won', 'lost')),
  priority text not null default 'medium' check (priority in ('low', 'medium', 'high', 'urgent')),
  source text,
  budget_amount numeric(12, 2) not null default 0 check (budget_amount >= 0),
  required_date date,
  notes text,
  products jsonb not null default '[]'::jsonb,
  user_id uuid not null default auth.uid() references auth.users(id) on delete cascade,
  created_at timestamptz not null default timezone('utc', now()),
  updated_at timestamptz not null default timezone('utc', now()),
  constraint inquiries_company_fk foreign key (company_id) references public.companies(id) on delete cascade,
  constraint inquiries_company_user_fk foreign key (company_id, user_id) references public.companies(id, user_id) on delete cascade,
  constraint inquiries_contact_user_fk foreign key (contact_id, user_id) references public.contacts(id, user_id) on delete set null
);

create table if not exists public.quotations (
  id uuid primary key default gen_random_uuid(),
  inquiry_id uuid,
  company_id uuid not null,
  status text not null default 'draft' check (status in ('draft', 'sent', 'revised', 'accepted', 'expired')),
  valid_until date,
  discount numeric(5, 2) not null default 0 check (discount >= 0 and discount <= 100),
  total_amount numeric(12, 2) not null default 0 check (total_amount >= 0),
  payment_terms text,
  products jsonb not null default '[]'::jsonb,
  user_id uuid not null default auth.uid() references auth.users(id) on delete cascade,
  created_at timestamptz not null default timezone('utc', now()),
  updated_at timestamptz not null default timezone('utc', now()),
  constraint quotations_company_fk foreign key (company_id) references public.companies(id) on delete cascade,
  constraint quotations_company_user_fk foreign key (company_id, user_id) references public.companies(id, user_id) on delete cascade,
  constraint quotations_inquiry_user_fk foreign key (inquiry_id, user_id) references public.inquiries(id, user_id) on delete set null
);

alter table public.orders
  add column if not exists products jsonb not null default '[]'::jsonb;

create index if not exists inquiries_user_id_idx on public.inquiries (user_id);
create index if not exists inquiries_company_id_idx on public.inquiries (company_id);
create index if not exists inquiries_status_idx on public.inquiries (user_id, status);
create index if not exists inquiries_products_gin_idx on public.inquiries using gin (products);

create index if not exists quotations_user_id_idx on public.quotations (user_id);
create index if not exists quotations_company_id_idx on public.quotations (company_id);
create index if not exists quotations_status_idx on public.quotations (user_id, status);
create index if not exists quotations_products_gin_idx on public.quotations using gin (products);

create index if not exists orders_products_gin_idx on public.orders using gin (products);

drop trigger if exists set_inquiries_updated_at on public.inquiries;
create trigger set_inquiries_updated_at
before update on public.inquiries
for each row
execute function public.set_updated_at();

drop trigger if exists set_quotations_updated_at on public.quotations;
create trigger set_quotations_updated_at
before update on public.quotations
for each row
execute function public.set_updated_at();

alter table public.inquiries enable row level security;
alter table public.quotations enable row level security;
alter table public.orders enable row level security;

alter table public.inquiries force row level security;
alter table public.quotations force row level security;
alter table public.orders force row level security;

drop policy if exists "users_manage_own_inquiries" on public.inquiries;
create policy "users_manage_own_inquiries"
on public.inquiries
for all
using (auth.uid() = user_id)
with check (auth.uid() = user_id);

drop policy if exists "users_manage_own_quotations" on public.quotations;
create policy "users_manage_own_quotations"
on public.quotations
for all
using (auth.uid() = user_id)
with check (auth.uid() = user_id);

drop policy if exists "users_manage_own_orders" on public.orders;
create policy "users_manage_own_orders"
on public.orders
for all
using (auth.uid() = user_id)
with check (auth.uid() = user_id);

grant select, insert, update, delete on public.inquiries to authenticated;
grant select, insert, update, delete on public.quotations to authenticated;
grant select, insert, update, delete on public.orders to authenticated;

commit;
