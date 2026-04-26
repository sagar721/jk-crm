import { createClient } from "@supabase/supabase-js";

function unwrap(result) {
  if (result.error) {
    throw result.error;
  }
  return result.data;
}

export function createCrmClient(url, anonKey) {
  if (!url || !anonKey) {
    throw new Error("Missing Supabase URL or anon key.");
  }

  return createClient(url, anonKey, {
    auth: {
      autoRefreshToken: true,
      persistSession: true,
      detectSessionInUrl: true,
    },
  });
}

export async function signUpWithEmail(supabase, email, password) {
  return unwrap(await supabase.auth.signUp({ email, password }));
}

export async function signInWithEmail(supabase, email, password) {
  return unwrap(await supabase.auth.signInWithPassword({ email, password }));
}

export async function signOut(supabase) {
  const { error } = await supabase.auth.signOut();
  if (error) {
    throw error;
  }
}

export async function createCompany(supabase, input) {
  return unwrap(
    await supabase
      .from("companies")
      .insert({
        name: input.name,
        industry: input.industry ?? null,
        size: input.size ?? null,
        website: input.website ?? null,
        location: input.location ?? null,
        status: input.status ?? "lead",
      })
      .select()
      .single()
  );
}

export async function fetchCompanies(supabase) {
  return unwrap(
    await supabase
      .from("companies")
      .select("id, name, industry, size, website, location, status, created_at")
      .order("created_at", { ascending: false })
  );
}

export async function updateCompany(supabase, companyId, updates) {
  return unwrap(
    await supabase
      .from("companies")
      .update(updates)
      .eq("id", companyId)
      .select()
      .single()
  );
}

export async function createContact(supabase, input) {
  return unwrap(
    await supabase
      .from("contacts")
      .insert({
        name: input.name,
        email: input.email,
        phone: input.phone ?? null,
        company_id: input.companyId,
        status: input.status ?? "lead",
      })
      .select()
      .single()
  );
}

export async function fetchContacts(supabase) {
  return unwrap(
    await supabase
      .from("contacts")
      .select(`
        id,
        name,
        email,
        phone,
        status,
        created_at,
        company:companies (
          id,
          name,
          status
        )
      `)
      .order("created_at", { ascending: false })
  );
}

export async function updateContact(supabase, contactId, updates) {
  return unwrap(
    await supabase
      .from("contacts")
      .update(updates)
      .eq("id", contactId)
      .select()
      .single()
  );
}

export async function createOrder(supabase, input) {
  return unwrap(
    await supabase
      .from("orders")
      .insert({
        amount: input.amount,
        status: input.status ?? "pending",
        company_id: input.companyId,
      })
      .select()
      .single()
  );
}

export async function fetchOrders(supabase) {
  return unwrap(
    await supabase
      .from("orders")
      .select(`
        id,
        amount,
        status,
        created_at,
        company:companies (
          id,
          name
        )
      `)
      .order("created_at", { ascending: false })
  );
}

export async function updateOrder(supabase, orderId, updates) {
  return unwrap(
    await supabase
      .from("orders")
      .update(updates)
      .eq("id", orderId)
      .select()
      .single()
  );
}
