# Supabase CRM Backend

This folder contains the production-ready Supabase schema for the CRM and a small `supabase-js` example client.

## What is covered

- Email/password authentication through Supabase Auth
- No password column in any application table
- Per-user ownership on `companies`, `contacts`, and `orders`
- Row Level Security enabled and forced on every CRM table
- `user_id` defaults to `auth.uid()`
- Foreign keys from `contacts.company_id` and `orders.company_id` to `companies.id`
- Composite tenant-safe foreign keys so contacts and orders cannot point at another user's company
- Case-insensitive unique email enforcement on `contacts`
- `created_at` and `updated_at` audit timestamps

## Apply the migration

Run the SQL in [20260425213000_crm_backend.sql](</Users/sagarmali/Downloads/CODEX CRM/supabase/migrations/20260425213000_crm_backend.sql>) with the Supabase SQL editor, or use the CLI:

```bash
supabase db push
```

## JavaScript usage

Install the client package in the app that will talk to Supabase:

```bash
npm install @supabase/supabase-js
```

Then use [crm-client.mjs](</Users/sagarmali/Downloads/CODEX CRM/supabase/examples/crm-client.mjs>). The examples intentionally omit `user_id` on inserts because the database fills it from the authenticated session.

## Security notes

- Use the anon key in browser code and let Supabase Auth create the user session.
- Do not expose the service role key in the browser.
- Keep user-scoped CRUD requests on an authenticated client so RLS can enforce `auth.uid() = user_id`.
