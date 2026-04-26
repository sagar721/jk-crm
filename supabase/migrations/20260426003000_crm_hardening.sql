begin;

create or replace function public.crm_products_are_valid(payload jsonb)
returns boolean
language plpgsql
as $$
declare
  item jsonb;
  qty numeric;
  unit_price numeric;
begin
  if payload is null or jsonb_typeof(payload) <> 'array' or jsonb_array_length(payload) = 0 then
    return false;
  end if;

  for item in select value from jsonb_array_elements(payload)
  loop
    if coalesce(trim(item->>'category'), trim(item->>'product'), '') = '' then
      return false;
    end if;
    qty := coalesce((item->>'qty')::numeric, 0);
    unit_price := coalesce((item->>'unitPrice')::numeric, (item->>'unit')::numeric, 0);
    if qty <= 0 or unit_price <= 0 then
      return false;
    end if;
  end loop;

  return true;
exception
  when others then
    return false;
end;
$$;

create or replace function public.crm_products_subtotal(payload jsonb)
returns numeric
language sql
immutable
as $$
  select coalesce(
    sum(
      greatest(coalesce((item->>'qty')::numeric, 0), 0) *
      greatest(coalesce((item->>'unitPrice')::numeric, (item->>'unit')::numeric, 0), 0)
    ),
    0
  )
  from jsonb_array_elements(coalesce(payload, '[]'::jsonb)) item
$$;

create or replace function public.crm_sync_document_totals()
returns trigger
language plpgsql
as $$
declare
  subtotal numeric;
  discount_amount numeric;
  taxable numeric;
  tax_rate numeric;
begin
  if not public.crm_products_are_valid(new.products) then
    raise exception 'Products must be a non-empty array with positive quantity and unit price.';
  end if;

  subtotal := public.crm_products_subtotal(new.products);
  if subtotal <= 0 then
    raise exception 'Document total must be greater than 0.';
  end if;

  if tg_table_name = 'inquiries' then
    new.total_amount := subtotal;
    new.budget_amount := greatest(coalesce(new.budget_amount, 0), subtotal);
    return new;
  end if;

  if tg_table_name = 'quotations' then
    discount_amount := subtotal * greatest(least(coalesce(new.discount, 0), 100), 0) / 100;
    taxable := greatest(subtotal - discount_amount, 0);
    new.total_amount := round(taxable * 1.18, 2);
    return new;
  end if;

  if tg_table_name = 'orders' then
    tax_rate := greatest(coalesce(new.tax_rate, 18), 0);
    discount_amount := subtotal * greatest(least(coalesce(new.discount_percentage, 0), 100), 0) / 100;
    taxable := greatest(subtotal - discount_amount, 0);
    new.amount := round(taxable * (1 + (tax_rate / 100)), 2);
    return new;
  end if;

  return new;
end;
$$;

do $$
begin
  if not exists (
    select 1 from pg_constraint where conname = 'inquiries_id_user_id_key'
  ) then
    alter table public.inquiries
      add constraint inquiries_id_user_id_key unique (id, user_id);
  end if;
  if not exists (
    select 1 from pg_constraint where conname = 'quotations_id_user_id_key'
  ) then
    alter table public.quotations
      add constraint quotations_id_user_id_key unique (id, user_id);
  end if;
  if not exists (
    select 1 from pg_constraint where conname = 'orders_id_user_id_key'
  ) then
    alter table public.orders
      add constraint orders_id_user_id_key unique (id, user_id);
  end if;
end;
$$;

alter table public.inquiries
  add column if not exists total_amount numeric(12, 2) not null default 0;

alter table public.quotations
  add column if not exists subtotal_amount numeric(12, 2) generated always as (public.crm_products_subtotal(products)) stored;

alter table public.orders
  add column if not exists quotation_id uuid,
  add column if not exists discount_percentage numeric(5, 2) not null default 0,
  add column if not exists tax_rate numeric(5, 2) not null default 18;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'orders_quotation_user_fk'
  ) then
    alter table public.orders
      add constraint orders_quotation_user_fk
      foreign key (quotation_id, user_id)
      references public.quotations(id, user_id)
      on delete set null;
  end if;
end;
$$;

alter table public.inquiries
  drop constraint if exists inquiries_products_valid_chk,
  add constraint inquiries_products_valid_chk check (public.crm_products_are_valid(products)),
  drop constraint if exists inquiries_total_amount_positive_chk,
  add constraint inquiries_total_amount_positive_chk check (total_amount > 0);

alter table public.quotations
  drop constraint if exists quotations_products_valid_chk,
  add constraint quotations_products_valid_chk check (public.crm_products_are_valid(products)),
  drop constraint if exists quotations_total_amount_positive_chk,
  add constraint quotations_total_amount_positive_chk check (total_amount > 0);

alter table public.orders
  drop constraint if exists orders_products_valid_chk,
  add constraint orders_products_valid_chk check (public.crm_products_are_valid(products)),
  drop constraint if exists orders_amount_positive_chk,
  add constraint orders_amount_positive_chk check (amount > 0),
  drop constraint if exists orders_discount_percentage_range_chk,
  add constraint orders_discount_percentage_range_chk check (discount_percentage >= 0 and discount_percentage <= 100),
  drop constraint if exists orders_tax_rate_non_negative_chk,
  add constraint orders_tax_rate_non_negative_chk check (tax_rate >= 0);

drop trigger if exists set_inquiries_document_totals on public.inquiries;
create trigger set_inquiries_document_totals
before insert or update of products, budget_amount
on public.inquiries
for each row
execute function public.crm_sync_document_totals();

drop trigger if exists set_quotations_document_totals on public.quotations;
create trigger set_quotations_document_totals
before insert or update of products, discount
on public.quotations
for each row
execute function public.crm_sync_document_totals();

drop trigger if exists set_orders_document_totals on public.orders;
create trigger set_orders_document_totals
before insert or update of products, discount_percentage, tax_rate
on public.orders
for each row
execute function public.crm_sync_document_totals();

create table if not exists public.inquiry_items (
  id uuid primary key default gen_random_uuid(),
  inquiry_id uuid not null,
  user_id uuid not null default auth.uid() references auth.users(id) on delete cascade,
  position integer not null default 1,
  product_name text not null,
  size text,
  material text,
  qty numeric(12, 2) not null check (qty > 0),
  unit_price numeric(12, 2) not null check (unit_price > 0),
  line_total numeric(12, 2) generated always as (qty * unit_price) stored,
  created_at timestamptz not null default timezone('utc', now()),
  updated_at timestamptz not null default timezone('utc', now()),
  constraint inquiry_items_parent_fk foreign key (inquiry_id, user_id) references public.inquiries(id, user_id) on delete cascade
);

create table if not exists public.quote_items (
  id uuid primary key default gen_random_uuid(),
  quotation_id uuid not null,
  user_id uuid not null default auth.uid() references auth.users(id) on delete cascade,
  position integer not null default 1,
  product_name text not null,
  size text,
  material text,
  qty numeric(12, 2) not null check (qty > 0),
  unit_price numeric(12, 2) not null check (unit_price > 0),
  line_total numeric(12, 2) generated always as (qty * unit_price) stored,
  created_at timestamptz not null default timezone('utc', now()),
  updated_at timestamptz not null default timezone('utc', now()),
  constraint quote_items_parent_fk foreign key (quotation_id, user_id) references public.quotations(id, user_id) on delete cascade
);

create table if not exists public.order_items (
  id uuid primary key default gen_random_uuid(),
  order_id uuid not null,
  user_id uuid not null default auth.uid() references auth.users(id) on delete cascade,
  position integer not null default 1,
  product_name text not null,
  size text,
  material text,
  qty numeric(12, 2) not null check (qty > 0),
  unit_price numeric(12, 2) not null check (unit_price > 0),
  line_total numeric(12, 2) generated always as (qty * unit_price) stored,
  created_at timestamptz not null default timezone('utc', now()),
  updated_at timestamptz not null default timezone('utc', now()),
  constraint order_items_parent_fk foreign key (order_id, user_id) references public.orders(id, user_id) on delete cascade
);

create index if not exists inquiries_user_company_idx on public.inquiries (user_id, company_id);
create index if not exists quotations_user_company_idx on public.quotations (user_id, company_id);
create index if not exists orders_user_company_idx on public.orders (user_id, company_id);
create index if not exists orders_quotation_id_idx on public.orders (quotation_id);

create index if not exists inquiry_items_user_inquiry_idx on public.inquiry_items (user_id, inquiry_id, position);
create index if not exists quote_items_user_quote_idx on public.quote_items (user_id, quotation_id, position);
create index if not exists order_items_user_order_idx on public.order_items (user_id, order_id, position);

drop trigger if exists set_inquiry_items_updated_at on public.inquiry_items;
create trigger set_inquiry_items_updated_at
before update on public.inquiry_items
for each row
execute function public.set_updated_at();

drop trigger if exists set_quote_items_updated_at on public.quote_items;
create trigger set_quote_items_updated_at
before update on public.quote_items
for each row
execute function public.set_updated_at();

drop trigger if exists set_order_items_updated_at on public.order_items;
create trigger set_order_items_updated_at
before update on public.order_items
for each row
execute function public.set_updated_at();

alter table public.inquiry_items enable row level security;
alter table public.quote_items enable row level security;
alter table public.order_items enable row level security;

alter table public.inquiry_items force row level security;
alter table public.quote_items force row level security;
alter table public.order_items force row level security;

drop policy if exists "users_manage_own_inquiry_items" on public.inquiry_items;
create policy "users_manage_own_inquiry_items"
on public.inquiry_items
for all
using (auth.role() = 'authenticated' and auth.uid() = user_id)
with check (auth.role() = 'authenticated' and auth.uid() = user_id);

drop policy if exists "users_manage_own_quote_items" on public.quote_items;
create policy "users_manage_own_quote_items"
on public.quote_items
for all
using (auth.role() = 'authenticated' and auth.uid() = user_id)
with check (auth.role() = 'authenticated' and auth.uid() = user_id);

drop policy if exists "users_manage_own_order_items" on public.order_items;
create policy "users_manage_own_order_items"
on public.order_items
for all
using (auth.role() = 'authenticated' and auth.uid() = user_id)
with check (auth.role() = 'authenticated' and auth.uid() = user_id);

drop policy if exists "users_manage_own_companies" on public.companies;
create policy "users_manage_own_companies"
on public.companies
for all
using (auth.role() = 'authenticated' and auth.uid() = user_id)
with check (auth.role() = 'authenticated' and auth.uid() = user_id);

drop policy if exists "users_manage_own_contacts" on public.contacts;
create policy "users_manage_own_contacts"
on public.contacts
for all
using (auth.role() = 'authenticated' and auth.uid() = user_id)
with check (auth.role() = 'authenticated' and auth.uid() = user_id);

drop policy if exists "users_manage_own_inquiries" on public.inquiries;
create policy "users_manage_own_inquiries"
on public.inquiries
for all
using (auth.role() = 'authenticated' and auth.uid() = user_id)
with check (auth.role() = 'authenticated' and auth.uid() = user_id);

drop policy if exists "users_manage_own_quotations" on public.quotations;
create policy "users_manage_own_quotations"
on public.quotations
for all
using (auth.role() = 'authenticated' and auth.uid() = user_id)
with check (auth.role() = 'authenticated' and auth.uid() = user_id);

drop policy if exists "users_manage_own_orders" on public.orders;
create policy "users_manage_own_orders"
on public.orders
for all
using (auth.role() = 'authenticated' and auth.uid() = user_id)
with check (auth.role() = 'authenticated' and auth.uid() = user_id);

revoke all on public.inquiries from anon;
revoke all on public.quotations from anon;
revoke all on public.orders from anon;
revoke all on public.inquiry_items from anon;
revoke all on public.quote_items from anon;
revoke all on public.order_items from anon;

grant select, insert, update, delete on public.inquiry_items to authenticated;
grant select, insert, update, delete on public.quote_items to authenticated;
grant select, insert, update, delete on public.order_items to authenticated;

commit;
