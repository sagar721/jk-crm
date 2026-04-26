const STORAGE_KEY = "jkfc-crm-state-v1";
const AUTH_STORAGE_KEY = "jkfc-crm-auth-v1";
const API_BASE = window.location.protocol === "file:" ? "http://127.0.0.1:8765" : "";
const DEFAULT_PAGE_SIZE = 50;
const COLLECTION_KEYS = ["users", "companies", "contacts", "stages", "inquiries", "products", "pipeline", "quotations", "quoteItems", "orders", "activities", "messages", "emails", "automations", "automationLog", "audit"];
const PAGE_COLLECTIONS = {
  dashboard: [],
  companies: ["companies"],
  contacts: ["companies", "contacts"],
  inquiries: ["companies", "contacts", "inquiries", "pipeline"],
  pipeline: ["companies", "inquiries", "pipeline", "stages"],
  quotations: ["companies", "inquiries", "quotations"],
  orders: ["companies", "quotations", "orders"],
  activities: ["companies", "activities"],
  whatsapp: ["contacts", "messages"],
  emails: ["emails"],
  automation: ["automations", "automationLog"],
  reports: ["companies", "inquiries", "pipeline", "quotations", "orders", "users"],
  assistant: ["contacts", "inquiries", "quotations", "orders"],
  settings: ["users", "stages", "audit"]
};

let backendStatus = { database: "offline", ai: "fallback", email: "simulated", whatsapp: "simulated", connected: false };
let remoteSaveTimer = null;
let aiGenerateDebounceTimer = null;
let aiSessionCalls = 0;
const AI_SESSION_LIMIT = 30;
let authState = loadAuthState();
let refreshPromise = null;
const actionLocks = new Set();
const collectionLoads = new Set();

const roles = {
  ADMIN: ["dashboard", "companies", "contacts", "inquiries", "pipeline", "quotations", "orders", "activities", "whatsapp", "emails", "automation", "reports", "assistant", "settings"],
  MANAGER: ["dashboard", "companies", "contacts", "inquiries", "pipeline", "quotations", "orders", "activities", "whatsapp", "emails", "automation", "reports", "assistant"],
  SALES: ["dashboard", "companies", "contacts", "inquiries", "pipeline", "quotations", "orders", "activities", "whatsapp", "emails", "assistant"],
  VIEWER: ["dashboard", "companies", "contacts", "inquiries", "orders", "assistant"]
};

const navItems = [
  ["dashboard", "Dashboard", "grid"],
  ["companies", "Companies", "building"],
  ["contacts", "Contacts", "users"],
  ["inquiries", "Inquiries", "inbox"],
  ["pipeline", "Pipeline", "kanban"],
  ["quotations", "Quotations", "file"],
  ["orders", "Orders", "truck"],
  ["activities", "Activities", "calendar"],
  ["whatsapp", "WhatsApp", "message"],
  ["emails", "Email Inbox", "mail"],
  ["automation", "Automation", "zap"],
  ["reports", "Reports", "chart"],
  ["assistant", "AI Assistant", "spark"],
  ["settings", "Settings", "settings"]
];

const iconPaths = {
  grid: "M3 3h7v7H3V3Zm11 0h7v7h-7V3ZM3 14h7v7H3v-7Zm11 0h7v7h-7v-7Z",
  building: "M4 21V4a1 1 0 0 1 1-1h10a1 1 0 0 1 1 1v17M3 21h18M8 7h1m0 4H8m0 4h1m5-8h1m0 4h-1m0 4h1",
  users: "M16 21v-2a4 4 0 0 0-4-4H6a4 4 0 0 0-4 4v2M9 11a4 4 0 1 0 0-8 4 4 0 0 0 0 8Zm13 10v-2a4 4 0 0 0-3-3.87M16 3.13a4 4 0 0 1 0 7.75",
  inbox: "M22 12h-6l-2 3h-4l-2-3H2l3-8h14l3 8Zm0 0v6a2 2 0 0 1-2 2H4a2 2 0 0 1-2-2v-6",
  kanban: "M4 4h5v16H4V4Zm7 0h5v10h-5V4Zm7 0h2v7h-2V4Z",
  file: "M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8l-6-6Zm0 0v6h6M8 13h8M8 17h6",
  truck: "M10 17H6a2 2 0 1 1-4 0H1V5h12v12h-1a2 2 0 1 1-2 0Zm9 0a2 2 0 1 1-4 0h-2V8h4l4 4v5h-2ZM17 8v4h4",
  calendar: "M8 2v4m8-4v4M3 10h18M5 4h14a2 2 0 0 1 2 2v14a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V6a2 2 0 0 1 2-2Z",
  message: "M21 11.5a8.4 8.4 0 0 1-9 8.3 8.5 8.5 0 0 1-4-.98L3 20l1.3-4.7a8.5 8.5 0 1 1 16.7-3.8Z",
  mail: "M4 4h16a2 2 0 0 1 2 2v12a2 2 0 0 1-2 2H4a2 2 0 0 1-2-2V6a2 2 0 0 1 2-2Zm18 2-10 7L2 6",
  zap: "M13 2 3 14h8l-1 8 11-14h-8l0-6Z",
  chart: "M3 3v18h18M7 16v-5m5 5V7m5 9v-8",
  spark: "M12 2l1.8 6.2L20 10l-6.2 1.8L12 18l-1.8-6.2L4 10l6.2-1.8L12 2Zm6 14 1 3 3 1-3 1-1 3-1-3-3-1 3-1 1-3Z",
  settings: "M12 15.5a3.5 3.5 0 1 0 0-7 3.5 3.5 0 0 0 0 7Zm8-3.5 2-1-2-3-2.2.8a7.5 7.5 0 0 0-1.4-.8L16 5h-4l-.4 3a7.5 7.5 0 0 0-1.4.8L8 8 6 11l2 1a8 8 0 0 0 0 2l-2 1 2 3 2.2-.8c.44.33.9.6 1.4.8l.4 3h4l.4-3c.5-.2.96-.47 1.4-.8L20 18l2-3-2-1a8 8 0 0 0 0-2Z"
};

const money = value => new Intl.NumberFormat("en-IN", { style: "currency", currency: "INR", maximumFractionDigits: 0 }).format(value || 0);
const today = () => new Date().toISOString().slice(0, 10);
const uid = prefix => `${prefix}-${Math.random().toString(36).slice(2, 8)}-${Date.now().toString(36)}`;
const byId = id => document.getElementById(id);

function collectionFlags(allLoaded = true) {
  return Object.fromEntries(COLLECTION_KEYS.map(key => [key, allLoaded]));
}

function loadAuthState() {
  try {
    const parsed = JSON.parse(localStorage.getItem(AUTH_STORAGE_KEY) || "{}");
    return {
      accessToken: String(parsed.accessToken || ""),
      refreshToken: String(parsed.refreshToken || ""),
      authProvider: String(parsed.authProvider || "")
    };
  } catch {
    return { accessToken: "", refreshToken: "", authProvider: "" };
  }
}

function saveAuthState() {
  localStorage.setItem(AUTH_STORAGE_KEY, JSON.stringify(authState));
}

function clearAuthState() {
  authState = { accessToken: "", refreshToken: "", authProvider: "", workspaceId: "" };
  localStorage.removeItem(AUTH_STORAGE_KEY);
}

function isActionLocked(key) {
  return actionLocks.has(key);
}

function isCollectionLoaded(name) {
  return Boolean(state.loadedCollections?.[name]);
}

function isCollectionLoading(name) {
  return collectionLoads.has(name);
}
const numberValue = value => {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
};

function blankProductLine() {
  return {
    id: uid("prd"),
    category: "",
    size: "",
    material: "",
    pressure: "",
    media: "",
    actuation: "",
    qty: 1,
    unitPrice: 0
  };
}

function normalizeProductLine(item = {}) {
  const base = blankProductLine();
  return {
    ...base,
    ...item,
    id: item.id || base.id,
    category: String(item.category || item.product || "").trim(),
    size: String(item.size || "").trim(),
    material: String(item.material || "").trim(),
    pressure: String(item.pressure || "").trim(),
    media: String(item.media || "").trim(),
    actuation: String(item.actuation || "").trim(),
    qty: Math.max(1, Math.round(numberValue(item.qty || 1))),
    unitPrice: Math.max(0, numberValue(item.unitPrice ?? item.unit ?? 0))
  };
}

function productLabel(item) {
  const line = normalizeProductLine(item);
  return [line.material, line.category, line.size].filter(Boolean).join(" ").trim() || line.category || "Product";
}

function lineTotal(item) {
  const line = normalizeProductLine(item);
  return line.qty * line.unitPrice;
}

function calculateProductTotals(products, discountPercent = 0) {
  const lines = (products || []).map(normalizeProductLine).filter(item => item.category);
  const subtotal = lines.reduce((sum, item) => sum + lineTotal(item), 0);
  const discount = subtotal * (Math.max(0, numberValue(discountPercent)) / 100);
  const taxable = Math.max(0, subtotal - discount);
  const gst = taxable * 0.18;
  return { subtotal, discount, gst, total: taxable + gst };
}

function inquiryProducts(entryOrId, appState = state) {
  const entry = typeof entryOrId === "string"
    ? (appState.inquiries || []).find(item => item.id === entryOrId)
    : entryOrId;
  if (!entry) return [];
  const direct = Array.isArray(entry.products) ? entry.products : [];
  const legacy = (appState.products || []).filter(item => item.inquiryId === entry.id);
  const source = direct.length ? direct : legacy;
  return source.map(normalizeProductLine).filter(item => item.category);
}

function quotationProducts(entryOrId, appState = state) {
  const entry = typeof entryOrId === "string"
    ? (appState.quotations || []).find(item => item.id === entryOrId)
    : entryOrId;
  if (!entry) return [];
  const direct = Array.isArray(entry.products) ? entry.products : [];
  const legacy = (appState.quoteItems || [])
    .filter(item => item.quotationId === entry.id)
    .map(item => ({
      id: item.productId || item.id,
      category: item.category || item.product,
      size: item.size,
      material: item.material,
      pressure: item.pressure,
      media: item.media,
      actuation: item.actuation,
      qty: item.qty,
      unitPrice: item.unit
    }));
  const fromInquiry = entry.inquiryId ? inquiryProducts(entry.inquiryId, appState) : [];
  const source = direct.length ? direct : legacy.length ? legacy : fromInquiry;
  return source.map(normalizeProductLine).filter(item => item.category);
}

function orderProducts(entryOrId, appState = state) {
  const entry = typeof entryOrId === "string"
    ? (appState.orders || []).find(item => item.id === entryOrId)
    : entryOrId;
  if (!entry) return [];
  const direct = Array.isArray(entry.products) ? entry.products : [];
  if (direct.length) return direct.map(normalizeProductLine).filter(item => item.category);
  const linkedQuote = (appState.quotations || []).find(item => item.id === entry.quotationId);
  return linkedQuote ? quotationProducts(linkedQuote, appState) : [];
}

function syncProductState(appState) {
  const nextState = { ...appState };
  const inquiries = (Array.isArray(nextState.inquiries) ? nextState.inquiries : []).map(item => ({
    ...item,
    products: inquiryProducts(item, nextState)
  }));
  const quotations = (Array.isArray(nextState.quotations) ? nextState.quotations : []).map(item => ({
    ...item,
    products: quotationProducts(item, { ...nextState, inquiries })
  }));
  const orders = (Array.isArray(nextState.orders) ? nextState.orders : []).map(item => {
    const linkedQuote = quotations.find(quote => quote.id === item.quotationId);
    const products = orderProducts(item, { ...nextState, inquiries, quotations });
    const totals = calculateProductTotals(products, linkedQuote?.discount || 0);
    const derivedValue = totals.total > 0 ? totals.total : numberValue(item.value || item.amount);
    return {
      ...item,
      products,
      value: derivedValue,
      amount: derivedValue
    };
  });
  return {
    ...nextState,
    inquiries,
    quotations,
    orders,
    products: inquiries.flatMap(item => item.products.map((product, index) => ({
      ...product,
      id: product.id || `ip-${item.id}-${index + 1}`,
      inquiryId: item.id
    }))),
    quoteItems: quotations.flatMap(item => item.products.map((product, index) => ({
      id: product.quoteItemId || `qi-${item.id}-${product.id || index + 1}`,
      quotationId: item.id,
      product: productLabel(product),
      hsn: product.hsn || "84818030",
      qty: product.qty,
      unit: product.unitPrice,
      brand: product.brand || "JK Fluid Controls",
      lead: product.lead || 14,
      size: product.size,
      material: product.material,
      pressure: product.pressure,
      media: product.media,
      actuation: product.actuation
    })))
  };
}

function inquiryTotals(entryOrId, appState = state) {
  return calculateProductTotals(inquiryProducts(entryOrId, appState));
}

function quoteTotalsForEntry(entry, appState = state) {
  return calculateProductTotals(quotationProducts(entry, appState), entry?.discount || 0);
}

function showToast(message, tone = "error") {
  let root = byId("toast-root");
  if (!root) {
    root = document.createElement("div");
    root.id = "toast-root";
    root.className = "toast-root";
    document.body.appendChild(root);
  }
  root.innerHTML = `<div class="toast ${tone}">${escapeHtml(message)}</div>`;
  clearTimeout(showToast.timer);
  showToast.timer = setTimeout(() => {
    if (root) root.innerHTML = "";
  }, 3600);
}

function setFormError(id, message) {
  const node = byId(id);
  if (node) node.textContent = message || "";
  if (message) showToast(message);
}

function seedState() {
  const users = [
    { id: "u-admin", name: "Sagar Mali", email: "admin@jkfluidcontrols.com", role: "ADMIN", phone: "+91 95378 20280", active: true },
    { id: "u-manager", name: "Nirali Shah", email: "manager@jkfluidcontrols.com", role: "MANAGER", phone: "+91 98765 11223", active: true },
    { id: "u-sales", name: "Amit Patel", email: "sales@jkfluidcontrols.com", role: "SALES", phone: "+91 98240 44331", active: true },
    { id: "u-viewer", name: "Support Desk", email: "viewer@jkfluidcontrols.com", role: "VIEWER", phone: "+91 99099 00881", active: true }
  ];
  const companies = [];
  const contacts = [];
  const stages = [
    { id: "s-new", name: "New Inquiry", color: "#2563eb", probability: 10 },
    { id: "s-review", name: "In Review", color: "#0f766e", probability: 25 },
    { id: "s-quoted", name: "Quotation Sent", color: "#b45309", probability: 45 },
    { id: "s-negotiation", name: "Negotiation", color: "#7c3aed", probability: 70 },
    { id: "s-won", name: "Won", color: "#15803d", probability: 100 }
  ];
  const inquiries = [];
  const products = [];
  const pipeline = [];
  const quotations = [];
  const quoteItems = [];
  const orders = [];
  const activities = [];
  const messages = [];
  const emails = [];
  const automations = [
    { id: "seq1", name: "Quotation Follow-up", trigger: "QUOTE_SENT", active: true, delayHours: 72, condition: "NO_REPLY", steps: "Day 3 email approval, Day 3 WhatsApp, Day 7 email, Day 14 manager alert" },
    { id: "seq2", name: "Post Delivery Check-in", trigger: "ORDER_DELIVERED", active: true, delayHours: 72, condition: "ALWAYS", steps: "Day 3 feedback, Day 30 check-in, Day 180 reorder suggestion" },
    { id: "seq3", name: "New Inquiry Acknowledgement", trigger: "INQUIRY_CREATED", active: true, delayHours: 0, condition: "ALWAYS", steps: "Instant WhatsApp acknowledgement, task creation for assigned sales rep" }
  ];
  return syncProductState({
    session: null,
    theme: "light",
    activePage: "dashboard",
    selectedContactId: "p1",
    users,
    companies,
    contacts,
    stages,
    inquiries,
    products,
    pipeline,
    quotations,
    quoteItems,
    orders,
    activities,
    messages,
    emails,
    automations,
    automationLog: [],
    audit: [{ id: "log1", user: "System", action: "Seeded CRM workspace", entity: "CRM", at: "2026-04-25 08:00" }],
    summary: {},
    pagination: {},
    loadedCollections: collectionFlags(true)
  });
}

let state = loadState();

function loadState() {
  const saved = localStorage.getItem(STORAGE_KEY);
  if (!saved) return seedState();
  try {
    const restored = normalizeState({ ...seedState(), ...JSON.parse(saved) });
    if (!authState.accessToken) restored.session = null;
    return restored;
  } catch {
    return seedState();
  }
}

function serializableState() {
  const { session, activePage, products, quoteItems, ...payload } = normalizeState(state);
  return payload;
}

function normalizeState(nextState) {
  const seeded = seedState();
  const merged = { ...seeded, ...nextState };
  if (!merged.loadedCollections || typeof merged.loadedCollections !== "object") {
    merged.loadedCollections = collectionFlags(true);
  } else {
    merged.loadedCollections = { ...collectionFlags(false), ...merged.loadedCollections };
  }
  if (!merged.pagination || typeof merged.pagination !== "object") merged.pagination = {};
  if (!merged.summary || typeof merged.summary !== "object") merged.summary = {};
  ["messages", "emails", "automations", "automationLog", "activities", "audit", "inquiries", "quotations", "orders", "products", "quoteItems"].forEach(key => {
    if (!Array.isArray(merged[key])) merged[key] = [];
  });
  seeded.automations.forEach(sequence => {
    if (!merged.automations.some(item => item.trigger === sequence.trigger)) merged.automations.push(sequence);
  });
  merged.automations = merged.automations.map(sequence => {
    const fallback = seeded.automations.find(item => item.trigger === sequence.trigger) || {};
    return {
      ...sequence,
      delayHours: sequence.delayHours ?? fallback.delayHours ?? 0,
      condition: sequence.condition || fallback.condition || "ALWAYS"
    };
  });
  const normalized = syncProductState(merged);
  if (!normalized.selectedContactId || !normalized.contacts.some(item => item.id === normalized.selectedContactId)) {
    normalized.selectedContactId = normalized.contacts[0]?.id || "";
  }
  return normalized;
}

function writeLocalState() {
  localStorage.setItem(STORAGE_KEY, JSON.stringify(state));
}

function canPersistRemotely() {
  return Boolean(state.session && authState.accessToken);
}

function saveState(options = {}) {
  state = normalizeState(state);
  writeLocalState();
  if (options.persistRemote !== false && canPersistRemotely()) scheduleRemoteSave();
}

async function refreshAccessToken() {
  if (!authState.refreshToken) throw new Error("Session expired");
  if (!refreshPromise) {
    refreshPromise = (async () => {
      const response = await fetch(`${API_BASE}/api/auth/refresh`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ refresh_token: authState.refreshToken })
      });
      const payload = await response.json().catch(() => ({}));
      if (!response.ok) throw new Error(payload.error || "Unable to refresh session");
      authState = { ...authState, accessToken: String(payload.access_token || "") };
      saveAuthState();
      return authState.accessToken;
    })().finally(() => {
      refreshPromise = null;
    });
  }
  return refreshPromise;
}

async function apiRequest(path, options = {}, retry = true) {
  const headers = { "Content-Type": "application/json", ...(options.headers || {}) };
  if (authState.accessToken) headers.Authorization = `Bearer ${authState.accessToken}`;
  if (authState.workspaceId) headers["X-Workspace-Id"] = authState.workspaceId;
  const response = await fetch(`${API_BASE}${path}`, {
    ...options,
    headers
  });
  const payload = await response.json().catch(() => ({}));
  if (response.status === 401 && retry && authState.refreshToken && !path.startsWith("/api/auth/")) {
    try {
      await refreshAccessToken();
      return apiRequest(path, options, false);
    } catch {
      clearAuthState();
      state = normalizeState({ ...state, session: null, activePage: "dashboard" });
      writeLocalState();
      render();
      throw new Error("Session expired");
    }
  }
  if (!response.ok) throw new Error(payload.error || `Request failed: ${response.status}`);
  return payload;
}

function scheduleRemoteSave() {
  clearTimeout(remoteSaveTimer);
  remoteSaveTimer = setTimeout(() => persistRemoteState(), 300);
}

async function persistRemoteState() {
  try {
    const response = await apiRequest("/api/state", {
      method: "PUT",
      body: JSON.stringify({ state: serializableState() })
    });
    if (response.state) state = normalizeState({ ...response.state, session: state.session, activePage: state.activePage });
    writeLocalState();
    backendStatus = { ...backendStatus, database: "connected", connected: true };
  } catch (error) {
    backendStatus = { ...backendStatus, database: "offline", connected: false };
    if (error?.message && state.session) showToast(error.message);
  }
}

function applyBootstrappedCollection(name, payload, append = false) {
  state = normalizeState({
    ...state,
    [name]: append ? [...(state[name] || []), ...(payload.items || [])] : (payload.items || []),
    pagination: { ...state.pagination, [name]: payload.pagination || {} },
    loadedCollections: { ...state.loadedCollections, [name]: true }
  });
  writeLocalState();
}

async function loadCollection(name, options = {}) {
  const append = Boolean(options.append);
  if (isCollectionLoading(name)) return;
  if (!append && isCollectionLoaded(name)) return;
  collectionLoads.add(name);
  render();
  try {
    const current = state.pagination?.[name] || {};
    const limit = options.limit || current.limit || DEFAULT_PAGE_SIZE;
    const offset = append ? ((current.offset || 0) + (current.limit || 0)) : 0;
    const payload = await apiRequest(`/api/data/${name}?limit=${limit}&offset=${offset}`);
    applyBootstrappedCollection(name, payload, append);
  } finally {
    collectionLoads.delete(name);
    render();
  }
}

async function loadMoreCollection(name) {
  return loadCollection(name, { append: true });
}

async function ensurePageData(page) {
  const required = PAGE_COLLECTIONS[page] || [];
  const pending = required.filter(name => !isCollectionLoaded(name));
  if (!pending.length) return;
  await Promise.all(pending.map(name => loadCollection(name)));
}

async function bootstrapSessionData(account) {
  const seeded = seedState();
  state = normalizeState({
    ...seeded,
    session: account,
    activePage: defaultPageForRole(account.role),
    companies: [],
    contacts: [],
    inquiries: [],
    products: [],
    pipeline: [],
    quotations: [],
    quoteItems: [],
    orders: [],
    activities: [],
    messages: [],
    emails: [],
    automationLog: [],
    audit: [],
    summary: {},
    pagination: {},
    loadedCollections: { ...collectionFlags(false), users: true, stages: true, automations: true }
  });
  writeLocalState();
  render();
  const [summary, companies, users, stages, automations] = await Promise.all([
    apiRequest("/api/summary"),
    apiRequest(`/api/data/companies?limit=${DEFAULT_PAGE_SIZE}&offset=0`),
    apiRequest("/api/data/users?limit=100&offset=0"),
    apiRequest("/api/data/stages?limit=50&offset=0"),
    apiRequest("/api/data/automations?limit=50&offset=0")
  ]);
  state = normalizeState({
    ...state,
    summary: summary.summary || {},
    companies: companies.items || [],
    users: users.items?.length ? users.items : state.users,
    stages: stages.items?.length ? stages.items : state.stages,
    automations: automations.items?.length ? automations.items : state.automations,
    pagination: {
      ...state.pagination,
      companies: companies.pagination || {},
      users: users.pagination || {},
      stages: stages.pagination || {},
      automations: automations.pagination || {}
    },
    loadedCollections: { ...state.loadedCollections, companies: true, users: true, stages: true, automations: true }
  });
  writeLocalState();
  render();
  await ensurePageData(state.activePage);
}

async function hydrateFromDatabase() {
  try {
    const health = await apiRequest("/api/health");
    backendStatus = { database: "connected", ai: health.ai || "fallback", email: health.email || "simulated", whatsapp: health.whatsapp || "simulated", connected: true };
    if (authState.accessToken) {
      const me = await apiRequest("/api/auth/me");
      await bootstrapSessionData(me.user || state.session || {});
      return;
    }
    render();
  } catch {
    backendStatus = { database: "offline", ai: "fallback", email: "simulated", whatsapp: "simulated", connected: false };
    clearAuthState();
    state = normalizeState({ ...state, session: null, activePage: "dashboard" });
    writeLocalState();
    render();
  }
}

function setState(patch, options = {}) {
  state = { ...state, ...patch };
  saveState(options);
  render();
}

function icon(name) {
  return `<svg viewBox="0 0 24 24" aria-hidden="true"><path d="${iconPaths[name] || iconPaths.grid}"/></svg>`;
}

function company(id) { return state.companies.find(item => item.id === id) || {}; }
function contact(id) { return state.contacts.find(item => item.id === id) || {}; }
function user(id) { return state.users.find(item => item.id === id) || {}; }
function inquiry(id) { return state.inquiries.find(item => item.id === id) || {}; }
function quoteTotals(quotationId) {
  const quote = state.quotations.find(item => item.id === quotationId) || { discount: 0 };
  return quoteTotalsForEntry(quote, state);
}

function accessiblePages() {
  if (!state.session) return [];
  return navItems.filter(([key]) => roles[state.session.role].includes(key));
}

async function routeTo(page) {
  state.activePage = page;
  saveState({ persistRemote: false });
  render();
  await ensurePageData(page);
}

function defaultPageForRole(role) {
  const pages = roles[role] || [];
  return pages.includes("dashboard") ? "dashboard" : pages[0] || "dashboard";
}

async function login(event) {
  event.preventDefault();
  const data = new FormData(event.target);
  const email = String(data.get("email")).trim().toLowerCase();
  const password = String(data.get("password") || "");
  const submitButton = event.target.querySelector("button[type='submit']");
  byId("login-error").textContent = "";
  if (submitButton) submitButton.disabled = true;
  try {
    const response = await apiRequest("/api/auth/login", {
      method: "POST",
      body: JSON.stringify({ email, password })
    });
    const account = response.user;
    authState = {
      accessToken: String(response.access_token || response.token || ""),
      refreshToken: String(response.refresh_token || ""),
      authProvider: String(response.auth_provider || "")
    };
    saveAuthState();
    backendStatus = { ...backendStatus, database: "connected", connected: true };
    state = normalizeState({ ...state, session: account, activePage: defaultPageForRole(account.role) });
    writeLocalState();
    await bootstrapSessionData(account);
  } catch (error) {
    byId("login-error").textContent = error.message || "Unable to sign in.";
  } finally {
    if (submitButton) submitButton.disabled = false;
  }
}

function logout() {
  clearAuthState();
  setState({ session: null, activePage: "dashboard" }, { persistRemote: false });
}

function render() {
  document.body.className = state.theme === "dark" ? "dark" : "";
  if (!state.session) {
    byId("app").innerHTML = renderLogin();
    return;
  }
  if (!roles[state.session.role].includes(state.activePage)) {
    state.activePage = roles[state.session.role][0];
  }
  byId("app").innerHTML = `
    <div class="shell">
      <aside class="sidebar">
        <div class="brand">
          <div class="brand-mark">JK</div>
          <div>
            <strong>JK Fluid Controls</strong>
            <span>CRM Platform</span>
          </div>
        </div>
        <nav>${accessiblePages().map(([key, label, iconName]) => `<button class="nav-item ${state.activePage === key ? "active" : ""}" onclick="routeTo('${key}')">${icon(iconName)}<span>${label}</span></button>`).join("")}</nav>
      </aside>
      <main class="main">
        <header class="topbar">
          <div>
            <p class="eyebrow">${state.session.role.replace("_", " ")}</p>
            <h1>${pageTitle(state.activePage)}</h1>
          </div>
          <div class="top-actions">
            <span class="status-pill ${backendStatus.connected ? "ok" : ""}">DB ${backendStatus.database}</span>
            <span class="status-pill">AI ${backendStatus.ai}</span>
            <span class="status-pill">WA ${backendStatus.whatsapp}</span>
            <span class="status-pill">Mail ${backendStatus.email}</span>
            <button class="icon-button" onclick="globalSearch()" title="Global search">${icon("spark")}</button>
            <button class="icon-button" onclick="toggleTheme()" title="Toggle theme">${icon("settings")}</button>
            <div class="user-pill"><span>${state.session.name}</span><button onclick="logout()">Logout</button></div>
          </div>
        </header>
        ${renderPage(state.activePage)}
      </main>
    </div>
    <div id="modal-root"></div>
  `;
}

function renderLogin() {
  return `
    <section class="login-screen">
      <div class="login-art">
        <div class="plant-scene">
          <div class="pipe p1"></div><div class="pipe p2"></div><div class="pipe p3"></div>
          <div class="valve v1"></div><div class="valve v2"></div><div class="gauge"></div>
        </div>
      </div>
      <form class="login-card" onsubmit="login(event)">
        <div class="brand login-brand"><div class="brand-mark">JK</div><div><strong>JK Fluid Controls</strong><span>Industrial CRM</span></div></div>
        <h1>Sign in</h1>
        <label>Email <input name="email" type="email" value="admin@jkfluidcontrols.com" required /></label>
        <label>Password <input name="password" type="password" value="demo123" required /></label>
        <button class="primary" type="submit">Login</button>
        <p class="connection-note">Database: ${backendStatus.database} · AI: ${backendStatus.ai} · WA: ${backendStatus.whatsapp} · Mail: ${backendStatus.email}</p>
        <p id="login-error" class="error"></p>
        <div class="demo-logins">
          <button type="button" onclick="quickLogin('admin@jkfluidcontrols.com')">Admin</button>
          <button type="button" onclick="quickLogin('manager@jkfluidcontrols.com')">Manager</button>
          <button type="button" onclick="quickLogin('sales@jkfluidcontrols.com')">Sales</button>
          <button type="button" onclick="quickLogin('viewer@jkfluidcontrols.com')">Viewer</button>
        </div>
      </form>
    </section>
  `;
}

function quickLogin(email) {
  const form = document.querySelector(".login-card");
  if (!form) return;
  const emailInput = form.querySelector("input[name='email']");
  const passwordInput = form.querySelector("input[name='password']");
  if (emailInput) emailInput.value = email;
  if (passwordInput && !passwordInput.value) passwordInput.value = "demo123";
  form.requestSubmit();
}

function pageTitle(page) {
  return (navItems.find(([key]) => key === page) || [page, page])[1];
}

function renderPage(page) {
  const pages = {
    dashboard: renderDashboard,
    companies: renderCompanies,
    contacts: renderContacts,
    inquiries: renderInquiries,
    pipeline: renderPipeline,
    quotations: renderQuotations,
    orders: renderOrders,
    activities: renderActivities,
    whatsapp: renderWhatsApp,
    emails: renderEmails,
    automation: renderAutomation,
    reports: renderReports,
    assistant: renderAssistant,
    settings: renderSettings
  };
  return pages[page]();
}

function renderDashboard() {
  const summary = state.summary || {};
  const counts = summary.counts || {};
  const openInquiry = isCollectionLoaded("inquiries") ? state.inquiries.filter(i => !["WON", "LOST"].includes(i.status)).length : numberValue(counts.openInquiries || counts.inquiries);
  const pipelineValue = isCollectionLoaded("pipeline") ? state.pipeline.reduce((sum, deal) => sum + deal.value, 0) : numberValue(summary.pipelineValue);
  const quoteValue = isCollectionLoaded("quotations") ? state.quotations.reduce((sum, q) => sum + quoteTotals(q.id).total, 0) : numberValue(summary.quoteValue);
  const overdue = isCollectionLoaded("activities") ? state.activities.filter(a => !a.done && a.due <= today()).length : numberValue(summary.overdueActivities);
  const funnel = isCollectionLoaded("pipeline")
    ? state.stages.map(stage => ({
      id: stage.id,
      name: stage.name,
      count: state.pipeline.filter(deal => deal.stageId === stage.id).length
    }))
    : (summary.funnel || []).map(item => ({ id: item.id, name: item.name, count: item.count }));
  return `
    <section class="kpi-grid">
      ${kpi("Open inquiries", openInquiry, "+12% this month", "inbox")}
      ${kpi("Pipeline value", money(pipelineValue), "Weighted view available", "kanban")}
      ${kpi("Quote value", money(quoteValue), "GST included", "file")}
      ${kpi("Due follow-ups", overdue, "Needs attention today", "calendar")}
    </section>
    <section class="dashboard-grid">
      <div class="panel wide">
        <div class="panel-head"><h2>Revenue Trend</h2><button onclick="exportCsv('dashboard')">Export</button></div>
        <div class="bars">${[45, 58, 51, 76, 69, 88, 94, 86, 102, 118, 126, 141].map((h, i) => `<span style="height:${h}px"><b>${["May","Jun","Jul","Aug","Sep","Oct","Nov","Dec","Jan","Feb","Mar","Apr"][i]}</b></span>`).join("")}</div>
      </div>
      <div class="panel">
        <div class="panel-head"><h2>Funnel</h2></div>
        ${(funnel.length ? funnel : state.stages.map(stage => ({ id: stage.id, name: stage.name, count: 0 }))).map(s => {
          const stage = state.stages.find(item => item.id === s.id) || {};
          return `<div class="funnel-row"><span style="background:${stage.color || "#2563eb"}"></span><b>${s.name}</b><em>${s.count}</em></div>`;
        }).join("")}
      </div>
      <div class="panel">
        <div class="panel-head"><h2>Recent Activities</h2><button onclick="openActivityModal()">Add</button></div>
        ${isCollectionLoaded("activities") ? state.activities.slice(0, 5).map(activityCard).join("") : `<p class="form-note">Activities load on demand.</p>`}
      </div>
    </section>
  `;
}

function kpi(label, value, hint, iconName) {
  return `<article class="kpi"><div>${icon(iconName)}</div><span>${label}</span><strong>${value}</strong><em>${hint}</em></article>`;
}

function renderLoadMore(name) {
  const page = state.pagination?.[name] || {};
  if (!page.hasMore) return "";
  const shown = Array.isArray(state[name]) ? state[name].length : 0;
  return `<div class="list-footer"><button onclick="loadMoreCollection('${name}')">Load more</button><small>${shown} of ${page.total}</small></div>`;
}

function renderCompanies() {
  if (!isCollectionLoaded("companies") && isCollectionLoading("companies")) {
    return `<section class="panel"><p>Loading companies...</p></section>`;
  }
  return `
    <section class="toolbar">
      <input id="company-search" placeholder="Search companies, city, industry" oninput="filterTable('company')" />
      ${canWrite() ? `<button class="primary" onclick="openCompanyModal()">New Company</button>` : ""}
    </section>
    <section class="cards-grid" id="company-list">
      ${state.companies.map(c => `
        <article class="record-card company-row" data-search="${[c.name, c.city, c.industry, c.status].join(" ").toLowerCase()}">
          <div class="record-top"><h3>${c.name}</h3><span class="badge ${c.status.toLowerCase()}">${c.status}</span></div>
          <p>${c.industry} · ${c.city}, ${c.state}</p>
          <div class="record-meta"><span>${c.email}</span><span>${user(c.assignedTo).name || "Unassigned"}</span></div>
          <div class="tag-row">${c.tags.map(t => `<span>${t}</span>`).join("")}</div>
          <button onclick="openCompanyDetail('${c.id}')">Open Profile</button>
        </article>`).join("")}
    </section>
    ${renderLoadMore("companies")}
  `;
}

function renderContacts() {
  if (!isCollectionLoaded("contacts") && isCollectionLoading("contacts")) {
    return `<section class="panel"><p>Loading contacts...</p></section>`;
  }
  return `
    <section class="toolbar">
      <input id="contact-search" placeholder="Search contacts, company, designation" oninput="filterTable('contact')" />
      ${canWrite() ? `<button class="primary" onclick="openContactModal()">New Contact</button>` : ""}
    </section>
    <section class="table-panel">
      <table><thead><tr><th>Name</th><th>Company</th><th>Designation</th><th>Email</th><th>WhatsApp</th><th>Consent</th></tr></thead>
      <tbody>${state.contacts.map(p => `<tr class="contact-row" data-search="${[p.first, p.last, company(p.companyId).name, p.designation, p.email].join(" ").toLowerCase()}"><td><button class="link" onclick="openContactDetail('${p.id}')">${p.first} ${p.last}</button></td><td>${company(p.companyId).name}</td><td>${p.designation}</td><td>${p.email}</td><td>${p.whatsapp}</td><td>${p.waOptIn ? "Opted in" : "No"}</td></tr>`).join("")}</tbody></table>
    </section>
    ${renderLoadMore("contacts")}
  `;
}

function renderInquiries() {
  if (!isCollectionLoaded("inquiries") && isCollectionLoading("inquiries")) {
    return `<section class="panel"><p>Loading inquiries...</p></section>`;
  }
  return `
    <section class="toolbar">
      <div class="segmented">${["ALL", "NEW", "IN_REVIEW", "QUOTED", "NEGOTIATION", "WON"].map(s => `<button onclick="filterStatus('${s}')">${s.replace("_", " ")}</button>`).join("")}</div>
      ${canWrite() ? `<button class="primary" onclick="openInquiryModal()">New Inquiry</button>` : ""}
    </section>
    <section class="table-panel">
      <table><thead><tr><th>No.</th><th>Company</th><th>Priority</th><th>Status</th><th>Source</th><th>Budget</th><th>Required</th><th></th></tr></thead>
      <tbody id="inquiry-table">${state.inquiries.map(i => inquiryRow(i)).join("")}</tbody></table>
    </section>
    ${renderLoadMore("inquiries")}
  `;
}

function inquiryRow(i) {
  const totals = inquiryTotals(i);
  const readyToConvert = inquiryProducts(i).length > 0 && totals.total > 0;
  const locked = isActionLocked(`convert:${i.id}`);
  return `<tr data-status="${i.status}"><td><button class="link" onclick="openInquiryDetail('${i.id}')">${i.no}</button></td><td>${company(i.companyId).name}</td><td><span class="badge ${i.priority.toLowerCase()}">${i.priority}</span></td><td><span class="badge">${i.status.replace("_", " ")}</span></td><td>${i.source}</td><td>${money(totals.total || i.budgetMax)}</td><td>${i.requiredDate}</td><td>${canWrite() ? `<button ${readyToConvert && !locked ? "" : "disabled title='Add at least one priced product first'"} onclick="convertToQuote('${i.id}')">${locked ? "Converting..." : "Convert"}</button>` : ""}</td></tr>`;
}

function renderPipeline() {
  return `
    <section class="pipeline-board">
      ${state.stages.map(stage => {
        const deals = state.pipeline.filter(deal => deal.stageId === stage.id);
        const total = deals.reduce((sum, deal) => sum + deal.value, 0);
        return `<div class="stage" ondragover="event.preventDefault()" ondrop="dropDeal(event,'${stage.id}')">
          <div class="stage-head"><span style="background:${stage.color}"></span><div><h2>${stage.name}</h2><p>${deals.length} deals · ${money(total)}</p></div></div>
          ${deals.map(deal => {
            const i = inquiry(deal.inquiryId);
            return `<article class="deal-card" draggable="${canWrite()}" ondragstart="dragDeal(event,'${deal.id}')">
              <strong>${company(i.companyId).name}</strong>
              <span>${i.no} · ${i.priority}</span>
              <b>${money(deal.value)}</b>
              <small>Close ${deal.expectedClose}</small>
            </article>`;
          }).join("")}
        </div>`;
      }).join("")}
    </section>
  `;
}

function renderQuotations() {
  if (!isCollectionLoaded("quotations") && isCollectionLoading("quotations")) {
    return `<section class="panel"><p>Loading quotations...</p></section>`;
  }
  return `
    <section class="toolbar">
      ${canWrite() ? `<button class="primary" onclick="openQuotationModal()">New Quotation</button>` : ""}
      <button onclick="printQuotation()">Print Preview</button>
    </section>
    <section class="cards-grid">
      ${state.quotations.map(q => {
        const totals = quoteTotals(q.id);
        return `<article class="record-card">
          <div class="record-top"><h3>${q.no}</h3><span class="badge">${q.status}</span></div>
          <p>${company(q.companyId).name}</p>
          <div class="quote-total">${money(totals.total)}</div>
          <div class="record-meta"><span>Valid ${q.validUntil}</span><span>GST ${money(totals.gst)}</span></div>
          <div class="button-row"><button onclick="openQuotationDetail('${q.id}')">Builder</button><button onclick="sendQuote('${q.id}')">Send</button><button onclick="printQuotation('${q.id}')">PDF</button></div>
        </article>`;
      }).join("")}
    </section>
    ${renderLoadMore("quotations")}
  `;
}

function renderOrders() {
  if (!isCollectionLoaded("orders") && isCollectionLoading("orders")) {
    return `<section class="panel"><p>Loading orders...</p></section>`;
  }
  return `
    <section class="toolbar">${canWrite() ? `<button class="primary" onclick="openOrderModal()">New Order</button>` : ""}</section>
    <section class="table-panel">
      <table><thead><tr><th>Order</th><th>Company</th><th>PO</th><th>Status</th><th>Payment</th><th>Dispatch</th><th>Value</th></tr></thead>
      <tbody>${state.orders.map(o => `<tr><td>${o.no}</td><td>${company(o.companyId).name}</td><td>${o.po}</td><td>${statusStepper(o.status)}</td><td>${o.payment}</td><td>${o.courier || "-"} ${o.tracking || ""}</td><td>${money(o.value)}</td></tr>`).join("")}</tbody></table>
    </section>
    ${renderLoadMore("orders")}
  `;
}

function statusStepper(current) {
  return `<div class="stepper">${["CONFIRMED","PROCESSING","DISPATCHED","DELIVERED"].map(s => `<span class="${s === current ? "current" : ""}">${s}</span>`).join("")}</div>`;
}

function renderActivities() {
  const days = Array.from({ length: 14 }, (_, i) => {
    const d = new Date("2026-04-20");
    d.setDate(d.getDate() + i);
    return d.toISOString().slice(0, 10);
  });
  return `
    <section class="toolbar"><button class="primary" onclick="openActivityModal()">Quick Add Activity</button></section>
    <section class="activity-layout">
      <div class="panel">${state.activities.map(activityCard).join("")}</div>
      <div class="calendar-grid">${days.map(d => `<div><strong>${d.slice(5)}</strong>${state.activities.filter(a => a.due === d).map(a => `<span>${a.title}</span>`).join("")}</div>`).join("")}</div>
    </section>
  `;
}

function activityCard(a) {
  return `<article class="activity-card"><span class="badge">${a.type}</span><div><strong>${a.title}</strong><p>${company(a.companyId).name || "Internal"} · due ${a.due}</p><small>${a.outcome}</small></div>${canWrite() ? `<button onclick="toggleActivity('${a.id}')">${a.done ? "Done" : "Mark"}</button>` : ""}</article>`;
}

function renderWhatsApp() {
  const activeContact = contact(state.selectedContactId) || state.contacts[0] || {};
  const thread = state.messages.filter(m => m.contactId === activeContact.id);
  return `
    <section class="toolbar">
      <button class="primary" onclick="runAutomation()">Run Automations</button>
      <button onclick="simulateInboundWhatsApp()">Simulate Inbound</button>
      <button onclick="sendWhatsAppTemplate()">Send Template</button>
    </section>
    <section class="inbox-layout">
      <div class="conversation-list">${state.contacts.map(p => {
        const unread = state.messages.filter(m => m.contactId === p.id && m.direction === "IN").length;
        return `<button class="${p.id === activeContact.id ? "active" : ""}" onclick="selectConversation('${p.id}')"><strong>${p.first} ${p.last}</strong><span>${company(p.companyId).name} · ${unread} inbound</span></button>`;
      }).join("")}</div>
      <div class="chat-panel">
        <div class="chat-head"><h2>${activeContact.first || "No"} ${activeContact.last || "Contact"}</h2><span class="status-pill ok">Bot ON</span></div>
        <div class="messages">${thread.map(m => `<div class="msg ${m.direction.toLowerCase()}"><p>${escapeHtml(m.content)}</p><span>${m.time}${m.bot ? " · AI" : ""}${m.provider ? ` · ${m.provider}` : ""}</span></div>`).join("") || `<div class="ai-bubble">No WhatsApp messages for this contact yet.</div>`}</div>
        <form class="composer" onsubmit="sendMessage(event)"><input name="message" placeholder="Type WhatsApp reply" /><button class="primary">Send</button></form>
      </div>
    </section>
  `;
}

function renderEmails() {
  return `
    <section class="toolbar"><button class="primary" onclick="composeEmail()">Compose Email</button><button onclick="draftEmail()">AI Draft Email</button><button onclick="runAutomation()">Run Automations</button><button onclick="logEmail()">Log Inbound</button></section>
    <section class="table-panel">
      <table><thead><tr><th>From</th><th>To</th><th>Subject</th><th>Status</th><th>Provider</th><th>Linked Record</th><th>Time</th></tr></thead>
      <tbody>${state.emails.map(e => `<tr><td>${e.from || "-"}</td><td>${e.to || "-"}</td><td>${e.subject}</td><td><span class="badge">${e.status}</span></td><td>${e.provider || "local"}</td><td>${e.linked}</td><td>${e.time}</td></tr>`).join("")}</tbody></table>
    </section>
  `;
}

function renderAutomation() {
  const sentToday = state.automationLog.filter(item => String(item.at || "").slice(0, 10) === today()).length;
  const activeCount = state.automations.filter(seq => seq.active).length;
  return `
    <section class="kpi-grid automation-kpis">
      ${kpi("Active sequences", activeCount, "Quote, inquiry, order triggers", "zap")}
      ${kpi("Automation actions", state.automationLog.length, `${sentToday} today`, "mail")}
      ${kpi("WhatsApp messages", state.messages.length, backendStatus.whatsapp, "message")}
      ${kpi("Email logs", state.emails.length, backendStatus.email, "mail")}
    </section>
    <section class="toolbar"><button class="primary" onclick="runAutomation()">Run Now</button><button onclick="openAutomationModal()">New Sequence</button><button onclick="refreshCommunicationLogs()">Refresh Logs</button></section>
    <section class="cards-grid automation-grid">
      ${state.automations.map(seq => `<article class="record-card"><div class="record-top"><h3>${seq.name}</h3><label class="switch"><input aria-label="Toggle ${escapeHtml(seq.name)} automation" type="checkbox" ${seq.active ? "checked" : ""} onchange="toggleAutomation('${seq.id}')" /><span></span>${seq.active ? "Active" : "Paused"}</label></div><p>${seq.trigger}</p><div class="record-meta"><span>Delay: ${seq.delayHours || 0}h</span><span>Condition: ${seq.condition || "ALWAYS"}</span></div><small>${seq.steps}</small></article>`).join("")}
    </section>
    <section class="table-panel automation-log"><table><thead><tr><th>Time</th><th>Title</th><th>Channel</th><th>Status</th><th>Detail</th></tr></thead><tbody>${state.automationLog.map(item => `<tr><td>${item.at}</td><td>${item.title}</td><td>${item.channel}</td><td><span class="badge">${item.status}</span></td><td>${item.detail}</td></tr>`).join("") || `<tr><td colspan="5">No automation actions yet.</td></tr>`}</tbody></table></section>
  `;
}

function renderReports() {
  const byIndustry = state.companies.reduce((acc, c) => ((acc[c.industry] = (acc[c.industry] || 0) + 1), acc), {});
  return `
    <section class="toolbar"><input type="date" value="2026-04-01" /><input type="date" value="2026-04-25" /><button onclick="exportCsv('reports')">Export Excel CSV</button></section>
    <section class="dashboard-grid">
      <div class="panel"><h2>Industry Mix</h2>${Object.entries(byIndustry).map(([k, v]) => `<div class="report-row"><span>${k}</span><b>${v}</b></div>`).join("")}</div>
      <div class="panel"><h2>Sales Performance</h2>${state.users.filter(u => u.role !== "VIEWER").map(u => `<div class="report-row"><span>${u.name}</span><b>${money(state.pipeline.filter(d => inquiry(d.inquiryId).assignedTo === u.id).reduce((s, d) => s + d.value, 0))}</b></div>`).join("")}</div>
      <div class="panel wide"><h2>Conversion Snapshot</h2><div class="conversion">${state.stages.map(s => `<div><strong>${s.probability}%</strong><span>${s.name}</span></div>`).join("")}</div></div>
    </section>
  `;
}

function renderAssistant() {
  return `
    <section class="assistant-layout">
      <div class="quick-prompts">
        ${["Show overdue follow-ups", "Summarize pipeline", "Find inactive customers", "Draft quotation follow-up"].map(prompt => `<button onclick="askAssistant('${prompt}')">${prompt}</button>`).join("")}
      </div>
      <div class="assistant-actions"><button id="assistant-communication-btn" class="primary" onclick="openCommunicationModal()">AI Communication</button></div>
      <div class="assistant-chat" id="assistant-chat">
        <div class="ai-bubble">Ask about pipeline, follow-ups, quotations, or order status. I will use the current CRM data.</div>
      </div>
      <form class="composer" onsubmit="askAssistantForm(event)"><input name="question" placeholder="Ask the CRM assistant" /><button class="primary">Ask</button></form>
    </section>
  `;
}

function renderSettings() {
  return `
    <section class="settings-grid">
      <div class="panel"><h2>Users</h2>${state.users.map(u => `<div class="settings-row"><span>${u.name}<small>${u.email}</small></span><b>${u.role}</b></div>`).join("")}</div>
      <div class="panel"><h2>Pipeline Stages</h2>${state.stages.map(s => `<div class="settings-row"><span><i style="background:${s.color}"></i>${s.name}</span><b>${s.probability}%</b></div>`).join("")}</div>
      <div class="panel"><h2>Email Templates</h2>${["Quote follow-up", "Payment reminder", "Dispatch notice", "Post-delivery feedback"].map(t => `<div class="settings-row"><span>${t}</span><button>Edit</button></div>`).join("")}</div>
      <div class="panel"><h2>Audit Log</h2>${state.audit.slice(-6).reverse().map(l => `<div class="settings-row"><span>${l.action}<small>${l.entity} · ${l.at}</small></span><b>${l.user}</b></div>`).join("")}</div>
    </section>
  `;
}

function canWrite() {
  return ["ADMIN", "MANAGER", "SALES"].includes(state.session.role);
}

function filterTable(type) {
  const term = byId(`${type}-search`).value.toLowerCase();
  document.querySelectorAll(`.${type}-row`).forEach(row => row.hidden = !row.dataset.search.includes(term));
}

function filterStatus(status) {
  document.querySelectorAll("#inquiry-table tr").forEach(row => row.hidden = status !== "ALL" && row.dataset.status !== status);
}

function dragDeal(event, id) {
  event.dataTransfer.setData("text/plain", id);
}

function dropDeal(event, stageId) {
  if (!canWrite()) return;
  const id = event.dataTransfer.getData("text/plain");
  state.pipeline = state.pipeline.map(deal => deal.id === id ? { ...deal, stageId, movedAt: today() } : deal);
  state.audit.push({ id: uid("log"), user: state.session.name, action: "Moved pipeline deal", entity: id, at: new Date().toLocaleString() });
  saveState();
  render();
}

function toggleTheme() {
  setState({ theme: state.theme === "dark" ? "light" : "dark" });
}

function openModal(title, body) {
  byId("modal-root").innerHTML = `<div class="modal-backdrop" onclick="closeModal(event)"><section class="modal" onclick="event.stopPropagation()"><div class="panel-head"><h2>${title}</h2><button onclick="closeModal()">Close</button></div>${body}</section></div>`;
}

function closeModal(event) {
  if (!event || event.target.classList.contains("modal-backdrop")) byId("modal-root").innerHTML = "";
}

function openCompanyModal() {
  openModal("New Company", `<form class="form-grid" onsubmit="saveCompany(event)"><input name="name" placeholder="Company name" required /><input name="industry" placeholder="Industry" required /><input name="city" placeholder="City" required /><input name="state" placeholder="State" value="Gujarat" /><input name="email" placeholder="Email" /><input name="phone" placeholder="Phone" /><button class="primary">Create Company</button></form>`);
}

function saveCompany(event) {
  event.preventDefault();
  const data = Object.fromEntries(new FormData(event.target));
  state.companies.unshift({ id: uid("c"), ...data, country: "India", gst: "", status: "LEAD", size: "MEDIUM", assignedTo: state.session.id, tags: ["new"] });
  state.audit.push({ id: uid("log"), user: state.session.name, action: "Created company", entity: data.name, at: new Date().toLocaleString() });
  saveState();
  closeModal();
  render();
}

function openContactModal() {
  openModal("New Contact", `<form class="form-grid" onsubmit="saveContact(event)"><select name="companyId">${state.companies.map(c => `<option value="${c.id}">${c.name}</option>`).join("")}</select><input name="first" placeholder="First name" required /><input name="last" placeholder="Last name" required /><input name="designation" placeholder="Designation" /><input name="email" placeholder="Email" /><input name="phone" placeholder="Phone" /><input name="whatsapp" placeholder="WhatsApp" /><button class="primary">Create Contact</button></form>`);
}

function saveContact(event) {
  event.preventDefault();
  const data = Object.fromEntries(new FormData(event.target));
  state.contacts.unshift({ id: uid("p"), ...data, primary: false, waOptIn: true });
  saveState();
  closeModal();
  render();
}

function renderProductRow(line = blankProductLine()) {
  const product = normalizeProductLine(line);
  return `
    <div class="product-row">
      <input data-field="category" placeholder="Product" value="${escapeHtml(product.category)}" oninput="updateInquiryTotals()" required />
      <input data-field="size" placeholder="Size" value="${escapeHtml(product.size)}" oninput="updateInquiryTotals()" />
      <input data-field="material" placeholder="Material" value="${escapeHtml(product.material)}" oninput="updateInquiryTotals()" />
      <input data-field="qty" type="number" min="1" placeholder="Qty" value="${product.qty}" oninput="updateInquiryTotals()" required />
      <input data-field="unitPrice" type="number" min="0" step="0.01" placeholder="Unit price" value="${product.unitPrice}" oninput="updateInquiryTotals()" required />
      <div class="product-row-footer">
        <span class="product-line-total">${money(lineTotal(product))}</span>
        <button type="button" onclick="removeProductRow(this)">Remove</button>
      </div>
    </div>
  `;
}

function readProductRows(scope) {
  return [...scope.querySelectorAll(".product-row")]
    .map(row => normalizeProductLine({
      category: row.querySelector("[data-field='category']")?.value,
      size: row.querySelector("[data-field='size']")?.value,
      material: row.querySelector("[data-field='material']")?.value,
      qty: row.querySelector("[data-field='qty']")?.value,
      unitPrice: row.querySelector("[data-field='unitPrice']")?.value
    }))
    .filter(item => item.category);
}

function addProductRow(line = blankProductLine()) {
  const container = byId("inquiry-products");
  if (!container) return;
  container.insertAdjacentHTML("beforeend", renderProductRow(line));
  updateInquiryTotals();
}

function removeProductRow(button) {
  const container = byId("inquiry-products");
  if (!container) return;
  if (container.querySelectorAll(".product-row").length <= 1) {
    showToast("At least one product line is required.");
    return;
  }
  button.closest(".product-row")?.remove();
  updateInquiryTotals();
}

function updateInquiryTotals() {
  const container = byId("inquiry-products");
  if (!container) return;
  let subtotal = 0;
  [...container.querySelectorAll(".product-row")].forEach(row => {
    const product = normalizeProductLine({
      category: row.querySelector("[data-field='category']")?.value,
      size: row.querySelector("[data-field='size']")?.value,
      material: row.querySelector("[data-field='material']")?.value,
      qty: row.querySelector("[data-field='qty']")?.value,
      unitPrice: row.querySelector("[data-field='unitPrice']")?.value
    });
    const total = lineTotal(product);
    subtotal += total;
    const totalNode = row.querySelector(".product-line-total");
    if (totalNode) totalNode.textContent = money(total);
  });
  const totalNode = byId("inquiry-total");
  if (totalNode) totalNode.textContent = money(subtotal);
}

function openInquiryModal() {
  openModal("New Inquiry", `<form class="form-grid" onsubmit="saveInquiry(event)"><select name="companyId">${state.companies.map(c => `<option value="${c.id}">${c.name}</option>`).join("")}</select><select name="contactId">${state.contacts.map(p => `<option value="${p.id}">${p.first} ${p.last}</option>`).join("")}</select><select name="priority"><option>MEDIUM</option><option>HIGH</option><option>URGENT</option><option>LOW</option></select><select name="source"><option>WHATSAPP</option><option>WEBSITE</option><option>REFERRAL</option><option>COLD_CALL</option><option>EXHIBITION</option></select><input name="budgetMax" type="number" placeholder="Expected value" required /><input name="requiredDate" type="date" value="${today()}" /><textarea name="notes" placeholder="Requirement notes"></textarea><div class="product-builder"><div class="panel-head"><h3>Products</h3><button type="button" onclick="addProductRow()">Add Product</button></div><div id="inquiry-products">${renderProductRow()}</div><div class="product-summary"><span>Live total</span><strong id="inquiry-total">${money(0)}</strong></div></div><p id="inquiry-error" class="form-error"></p><button class="primary">Create Inquiry</button></form>`);
  updateInquiryTotals();
}

function saveInquiry(event) {
  event.preventDefault();
  const data = Object.fromEntries(new FormData(event.target));
  const products = readProductRows(event.target);
  const totals = calculateProductTotals(products);
  if (!products.length) {
    setFormError("inquiry-error", "Add at least one product before creating an inquiry.");
    return;
  }
  if (totals.total <= 0) {
    setFormError("inquiry-error", "Each inquiry needs a valid quantity and unit price.");
    return;
  }
  setFormError("inquiry-error", "");
  const next = String(state.inquiries.length + 1).padStart(5, "0");
  const id = uid("i");
  state.inquiries.unshift({ id, no: `JK-2026-${next}`, assignedTo: state.session.id, status: "NEW", projectType: "SPOT", budgetMin: 0, requirements: products.map(productLabel), createdAt: today(), ...data, budgetMax: Math.max(numberValue(data.budgetMax), totals.total), products });
  state.pipeline.push({ id: uid("d"), inquiryId: id, stageId: "s-new", value: totals.total, expectedClose: data.requiredDate, movedAt: today() });
  saveState();
  closeModal();
  render();
}

function convertToQuote(inquiryId) {
  const lockKey = `convert:${inquiryId}`;
  if (isActionLocked(lockKey)) return;
  const i = inquiry(inquiryId);
  const products = inquiryProducts(i);
  const totals = calculateProductTotals(products);
  if (!products.length) {
    showToast("Add at least one product line before converting this inquiry.");
    return;
  }
  if (totals.total <= 0) {
    showToast("Inquiry products need valid quantity and unit price before conversion.");
    return;
  }
  const existingQuote = state.quotations.find(item => item.inquiryId === inquiryId && !["EXPIRED"].includes(item.status));
  if (existingQuote) {
    showToast(`Quotation ${existingQuote.no} already exists for this inquiry.`, "success");
    state.activePage = "quotations";
    saveState({ persistRemote: false });
    render();
    return;
  }
  actionLocks.add(lockKey);
  render();
  try {
  const qid = uid("q");
  const no = `QT-2026-${String(state.quotations.length + 1).padStart(5, "0")}`;
  state.quotations.unshift({ id: qid, no, inquiryId, companyId: i.companyId, status: "DRAFT", validUntil: "2026-05-30", discount: 0, paymentTerms: "50% advance, balance before dispatch", sentAt: "", products: products.map(product => ({ ...product, quoteItemId: uid("qi"), lead: 21 })) });
  state.inquiries = state.inquiries.map(item => item.id === inquiryId ? { ...item, status: "QUOTED" } : item);
  state.pipeline = state.pipeline.map(deal => deal.inquiryId === inquiryId ? { ...deal, stageId: "s-quoted", value: totals.total, movedAt: today() } : deal);
  state.activePage = "quotations";
  saveState();
  render();
  } finally {
    actionLocks.delete(lockKey);
    render();
  }
}

function openQuotationModal() {
  const firstCompany = state.companies[0].id;
  openModal("New Quotation", `<form class="form-grid" onsubmit="saveQuotation(event)"><select name="companyId">${state.companies.map(c => `<option value="${c.id}">${c.name}</option>`).join("")}</select><input name="product" placeholder="Product" required /><input name="qty" type="number" placeholder="Qty" value="1" /><input name="unit" type="number" placeholder="Unit price" required /><input name="discount" type="number" placeholder="Discount %" value="0" /><button class="primary">Create Quotation</button></form>`);
}

function saveQuotation(event) {
  event.preventDefault();
  const data = Object.fromEntries(new FormData(event.target));
  const product = normalizeProductLine({ category: data.product, qty: data.qty, unitPrice: data.unit });
  if (!product.category || lineTotal(product) <= 0) {
    showToast("Quotation line item needs a product, quantity, and unit price.");
    return;
  }
  const qid = uid("q");
  state.quotations.unshift({ id: qid, no: `QT-2026-${String(state.quotations.length + 1).padStart(5, "0")}`, companyId: data.companyId, inquiryId: null, status: "DRAFT", validUntil: "2026-05-30", discount: Number(data.discount), paymentTerms: "50% advance, balance before dispatch", sentAt: "", products: [{ ...product, quoteItemId: uid("qi"), lead: 14 }] });
  saveState();
  closeModal();
  render();
}

function openQuotationDetail(id) {
  const q = state.quotations.find(item => item.id === id);
  const totals = quoteTotals(id);
  openModal(`Quotation ${q.no}`, `<div class="quote-preview"><h2>JK Fluid Controls</h2><p>Ahmedabad, India · sales@jkfluidcontrols.com · +91-95378 20280</p><h3>${escapeHtml(company(q.companyId).name)}</h3><table><thead><tr><th>Item</th><th>Qty</th><th>Unit</th><th>Total</th></tr></thead><tbody>${quotationProducts(q).map(item => `<tr><td>${escapeHtml(productLabel(item))}<small>${escapeHtml(item.material || "JK Fluid Controls")} · ${escapeHtml(item.size || "Std.")}</small></td><td>${item.qty}</td><td>${money(item.unitPrice)}</td><td>${money(lineTotal(item))}</td></tr>`).join("")}</tbody></table><div class="totals"><span>Subtotal ${money(totals.subtotal)}</span><span>Discount ${money(totals.discount)}</span><span>GST 18% ${money(totals.gst)}</span><strong>Total ${money(totals.total)}</strong></div><button class="primary" onclick="printQuotation('${id}')">Print / Save PDF</button></div>`);
}

async function sendQuote(id) {
  const q = state.quotations.find(item => item.id === id);
  const totals = quoteTotals(id);
  if (totals.total <= 0 || !quotationProducts(q).length) {
    showToast("Add priced product lines before sending a quotation.");
    return;
  }
  const c = company(q.companyId);
  const i = inquiry(q.inquiryId);
  const p = contact(i.contactId) || state.contacts.find(item => item.companyId === q.companyId) || {};
  const body = `Dear ${p.first || "Customer"},\n\nPlease find quotation ${q.no} from JK Fluid Controls. GST-inclusive total: ${money(totals.total)}. Valid until ${q.validUntil}.\n\nRegards,\nJK Fluid Controls`;
  state.quotations = state.quotations.map(item => item.id === id ? { ...item, status: "SENT", sentAt: today() } : item);
  try {
    const response = await apiRequest("/api/email/send", {
      method: "POST",
      body: JSON.stringify({ to: c.email, subject: `Quotation ${q.no}`, body, linked: q.no, state: serializableState() })
    });
    applyServerState(response.state);
    await apiRequest("/api/whatsapp/send", {
      method: "POST",
      body: JSON.stringify({ contactId: p.id, to: p.whatsapp || p.phone, content: `Quotation ${q.no} has been shared on email. GST-inclusive total: ${money(totals.total)}.`, linked: q.no, bot: true, state: serializableState() })
    }).then(res => applyServerState(res.state));
  } catch {
    state.emails.unshift({ id: uid("e"), from: "sales@jkfluidcontrols.com", to: c.email, subject: `Quotation ${q.no}`, status: "SENT", provider: "local", linked: q.no, time: new Date().toLocaleString(), body });
  }
  saveState();
  render();
}

function printQuotation(id) {
  if (id) openQuotationDetail(id);
  setTimeout(() => window.print(), 120);
}

function openOrderModal() {
  const availableQuotes = state.quotations.filter(q => quotationProducts(q).length && quoteTotals(q.id).total > 0 && !state.orders.some(order => order.quotationId === q.id));
  if (!availableQuotes.length) {
    showToast("Create a priced quotation before creating an order.");
    return;
  }
  openModal("New Order", `<form class="form-grid" onsubmit="saveOrder(event)"><select name="quotationId">${availableQuotes.map(q => `<option value="${q.id}">${q.no} - ${company(q.companyId).name} - ${money(quoteTotals(q.id).total)}</option>`).join("")}</select><input name="po" placeholder="PO number" required /><select name="status"><option>CONFIRMED</option><option>PROCESSING</option><option>DISPATCHED</option><option>DELIVERED</option></select><select name="payment"><option>PENDING</option><option>PARTIAL</option><option>PAID</option></select><input name="courier" placeholder="Courier" /><input name="tracking" placeholder="Tracking" /><p id="order-error" class="form-error"></p><button class="primary">Create Order</button></form>`);
}

function saveOrder(event) {
  event.preventDefault();
  const submitButton = event.target.querySelector("button[type='submit'], .primary");
  if (event.target.dataset.processing === "true") return;
  const data = Object.fromEntries(new FormData(event.target));
  const q = state.quotations.find(item => item.id === data.quotationId);
  if (!q) {
    setFormError("order-error", "Select a valid quotation before creating an order.");
    return;
  }
  const products = quotationProducts(q);
  const totals = quoteTotals(q.id);
  if (!q || !products.length) {
    setFormError("order-error", "Select a quotation with at least one product line.");
    return;
  }
  if (totals.total <= 0) {
    setFormError("order-error", "Cannot create an order from a zero-value quotation.");
    return;
  }
  if (state.orders.some(item => item.quotationId === q.id)) {
    setFormError("order-error", "An order already exists for this quotation.");
    return;
  }
  setFormError("order-error", "");
  event.target.dataset.processing = "true";
  if (submitButton) submitButton.disabled = true;
  try {
  state.orders.unshift({ id: uid("o"), no: `ORD-2026-${String(state.orders.length + 1).padStart(5, "0")}`, companyId: q.companyId, value: totals.total, amount: totals.total, dispatchDate: "", expectedDelivery: "", products: products.map(product => ({ ...product })), ...data });
  state.quotations = state.quotations.map(item => item.id === q.id ? { ...item, status: "ACCEPTED" } : item);
  saveState();
  closeModal();
  render();
  } finally {
    event.target.dataset.processing = "false";
    if (submitButton) submitButton.disabled = false;
  }
}

function openActivityModal() {
  openModal("Quick Add Activity", `<form class="form-grid" onsubmit="saveActivity(event)"><select name="type"><option>CALL</option><option>EMAIL</option><option>WA</option><option>MEETING</option><option>TASK</option><option>NOTE</option></select><input name="title" placeholder="Activity title" required /><select name="companyId">${state.companies.map(c => `<option value="${c.id}">${c.name}</option>`).join("")}</select><input name="due" type="date" value="${today()}" /><textarea name="outcome" placeholder="Notes or outcome"></textarea><button class="primary">Save Activity</button></form>`);
}

function saveActivity(event) {
  event.preventDefault();
  const data = Object.fromEntries(new FormData(event.target));
  state.activities.unshift({ id: uid("a"), owner: state.session.id, done: false, contactId: "", inquiryId: "", ...data });
  saveState();
  closeModal();
  render();
}

function toggleActivity(id) {
  state.activities = state.activities.map(a => a.id === id ? { ...a, done: !a.done } : a);
  saveState();
  render();
}

async function sendMessage(event) {
  event.preventDefault();
  const message = new FormData(event.target).get("message");
  if (!message) return;
  const p = contact(state.selectedContactId) || {};
  try {
    const response = await apiRequest("/api/whatsapp/send", {
      method: "POST",
      body: JSON.stringify({ contactId: p.id, to: p.whatsapp || p.phone, content: message, linked: "Manual WhatsApp", state: serializableState() })
    });
    applyServerState(response.state);
    backendStatus = { ...backendStatus, whatsapp: response.provider || backendStatus.whatsapp };
  } catch {
    state.messages.push({ id: uid("w"), contactId: p.id, direction: "OUT", content: message, time: new Date().toLocaleTimeString().slice(0, 5), bot: false, status: "SENT", provider: "local" });
    saveState();
  }
  render();
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

async function requestAI(path, prompt) {
  const response = await apiRequest(path, {
    method: "POST",
    body: JSON.stringify({ prompt, contactId: state.selectedContactId, state: serializableState() })
  });
  backendStatus = { ...backendStatus, ai: response.provider || backendStatus.ai, connected: true };
  return response.answer;
}

function applyServerState(nextState) {
  if (!nextState) return;
  state = normalizeState({ ...nextState, session: state.session, activePage: state.activePage, selectedContactId: state.selectedContactId || nextState.selectedContactId });
  backendStatus = { ...backendStatus, database: "connected", connected: true };
  writeLocalState();
}

async function draftEmail() {
  openModal("AI Draft Email", `<div class="ai-draft"><p>Generating draft from CRM context...</p><textarea>Loading...</textarea><button class="primary" onclick="closeModal()">Close</button></div>`);
  const prompt = "Draft a professional quotation follow-up email for the most urgent open CRM opportunity.";
  let draft;
  if (backendStatus.ai === "fallback") {
    draft = fallbackEmailDraft();
  } else {
    try {
      draft = await requestAI("/api/ai/email-draft", prompt);
    } catch {
      draft = fallbackEmailDraft();
    }
  }
  const c = state.companies[0] || {};
  openModal("AI Draft Email", `<form class="form-grid" onsubmit="sendEmailForm(event)"><input name="to" value="${escapeHtml(c.email || "")}" placeholder="To email" required /><input name="subject" value="${escapeHtml(firstEmailSubject(draft))}" placeholder="Subject" required /><textarea name="body">${escapeHtml(stripEmailSubject(draft))}</textarea><input name="linked" value="AI-DRAFT" placeholder="Linked record" /><button class="primary">Send Email</button></form>`);
}

function fallbackEmailDraft() {
  return "Subject: Follow-up on quotation and technical clarification\n\nDear Customer,\n\nThank you for reviewing our proposal. Based on your process requirement, we recommend proceeding with the quoted JK Fluid Controls valve package. Please let us know if you would like a revised commercial offer or any compliance documents.\n\nRegards,\nJK Fluid Controls";
}

function firstEmailSubject(draft) {
  const first = String(draft).split("\n").find(line => line.toLowerCase().startsWith("subject:"));
  return first ? first.replace(/subject:\s*/i, "").trim() : "Follow-up from JK Fluid Controls";
}

function stripEmailSubject(draft) {
  return String(draft).replace(/^Subject:.*(\r?\n){1,2}/i, "").trim();
}

function composeEmail() {
  const c = state.companies[0] || {};
  openModal("Compose Email", `<form class="form-grid" onsubmit="sendEmailForm(event)"><input name="to" value="${escapeHtml(c.email || "")}" placeholder="To email" required /><input name="subject" placeholder="Subject" required /><textarea name="body" placeholder="Email body" required></textarea><input name="linked" value="CRM" placeholder="Linked record" /><button class="primary">Send Email</button></form>`);
}

async function sendEmailForm(event) {
  event.preventDefault();
  const data = Object.fromEntries(new FormData(event.target));
  try {
    const response = await apiRequest("/api/email/send", {
      method: "POST",
      body: JSON.stringify({ ...data, state: serializableState() })
    });
    applyServerState(response.state);
    backendStatus = { ...backendStatus, email: response.provider || backendStatus.email };
  } catch {
    state.emails.unshift({ id: uid("e"), from: "sales@jkfluidcontrols.com", to: data.to, subject: data.subject, body: data.body, status: "SENT", provider: "local", linked: data.linked || "CRM", time: new Date().toLocaleString() });
    saveState();
  }
  closeModal();
  render();
}

function logEmail() {
  state.emails.unshift({ id: uid("e"), from: "customer@example.com", to: "sales@jkfluidcontrols.com", subject: "Inbound customer email", body: "Customer replied and sequence should pause for human review.", status: "OPEN", provider: "manual", linked: "CRM", time: new Date().toLocaleString() });
  state.activities.unshift({ id: uid("a"), type: "EMAIL", title: "Inbound customer email", companyId: state.companies[0]?.id || "", contactId: state.contacts[0]?.id || "", inquiryId: "", owner: state.session.id, due: today(), outcome: "Inbound email logged. Automation paused for review.", done: false });
  saveState();
  render();
}

function openAutomationModal() {
  openModal("New Automation Sequence", `<form class="form-grid" onsubmit="saveAutomation(event)"><input name="name" placeholder="Sequence name" required /><select name="trigger"><option>QUOTE_SENT</option><option>ORDER_DELIVERED</option><option>INQUIRY_CREATED</option></select><input name="delayHours" type="number" min="0" value="0" placeholder="Delay hours" /><select name="condition"><option>ALWAYS</option><option>NO_REPLY</option></select><textarea name="steps" placeholder="Steps"></textarea><button class="primary">Create Sequence</button></form>`);
}

function saveAutomation(event) {
  event.preventDefault();
  const data = Object.fromEntries(new FormData(event.target));
  state.automations.unshift({ id: uid("seq"), active: true, ...data, delayHours: Number(data.delayHours || 0) });
  saveState();
  closeModal();
  render();
}

function toggleAutomation(id) {
  state.automations = state.automations.map(seq => seq.id === id ? { ...seq, active: !seq.active } : seq);
  saveState();
  render();
}

async function runAutomation() {
  openModal("Running Automations", `<div class="ai-draft"><p>Processing active email and WhatsApp sequences...</p></div>`);
  try {
    const response = await apiRequest("/api/automation/run", {
      method: "POST",
      body: JSON.stringify({ state: serializableState() })
    });
    applyServerState(response.state);
    closeModal();
    render();
    openModal("Automation Complete", `<div class="ai-draft"><p>${response.count} automation action${response.count === 1 ? "" : "s"} completed.</p><div class="automation-result-list">${response.results.map(item => `<div class="settings-row"><span>${escapeHtml(item.title)}<small>${escapeHtml(item.detail)}</small></span><b>${item.channel}</b></div>`).join("") || "<p>No pending automation actions for today.</p>"}</div><button class="primary" onclick="closeModal()">Done</button></div>`);
  } catch (error) {
    closeModal();
    openModal("Automation Error", `<p>${escapeHtml(error.message)}</p>`);
  }
}

function simulateInboundWhatsApp() {
  const p = contact(state.selectedContactId) || state.contacts[0] || {};
  openModal("Simulate Inbound WhatsApp", `<form class="form-grid" onsubmit="submitInboundWhatsApp(event)"><p class="form-note">From ${escapeHtml(p.first || "selected contact")} ${escapeHtml(p.last || "")}</p><textarea name="content" required>Can you share quotation status and delivery time?</textarea><label class="switch"><input name="autoReply" type="checkbox" checked /><span></span> AI auto-reply</label><button class="primary">Receive Message</button></form>`);
}

async function submitInboundWhatsApp(event) {
  event.preventDefault();
  const p = contact(state.selectedContactId) || state.contacts[0] || {};
  const data = Object.fromEntries(new FormData(event.target));
  const content = data.content;
  if (!content) return;
  try {
    const response = await apiRequest("/api/whatsapp/inbound", {
      method: "POST",
      body: JSON.stringify({ contactId: p.id, content, autoReply: Boolean(data.autoReply), state: serializableState() })
    });
    applyServerState(response.state);
  } catch {
    state.messages.push({ id: uid("w"), contactId: p.id, direction: "IN", content, time: new Date().toLocaleTimeString().slice(0, 5), bot: false, status: "RECEIVED", provider: "local" });
    state.messages.push({ id: uid("w"), contactId: p.id, direction: "OUT", content: "Thanks for your message. Our team will check the CRM record and reply shortly.", time: new Date().toLocaleTimeString().slice(0, 5), bot: true, status: "SENT", provider: "local" });
    saveState();
  }
  closeModal();
  render();
}

async function sendWhatsAppTemplate() {
  const p = contact(state.selectedContactId) || state.contacts[0] || {};
  const text = `Hello ${p.first || ""}, this is JK Fluid Controls. We are following up on your valve requirement. Please reply with any clarification or updated delivery timeline.`;
  try {
    const response = await apiRequest("/api/whatsapp/send", {
      method: "POST",
      body: JSON.stringify({ contactId: p.id, to: p.whatsapp || p.phone, content: text, linked: "WA-TEMPLATE", bot: true, state: serializableState() })
    });
    applyServerState(response.state);
  } catch {
    state.messages.push({ id: uid("w"), contactId: p.id, direction: "OUT", content: text, time: new Date().toLocaleTimeString().slice(0, 5), bot: true, status: "SENT", provider: "local" });
    saveState();
  }
  render();
}

async function refreshCommunicationLogs() {
  try {
    const response = await apiRequest("/api/automation/logs");
    const imported = response.logs.map(item => ({
      id: uid("auto"),
      key: `${item.channel}:${item.created_at}:${item.recipient}`,
      title: item.subject || item.content.slice(0, 60),
      channel: item.channel,
      status: item.status,
      detail: `${item.provider} · ${item.recipient}`,
      at: item.created_at
    }));
    const existing = new Set(state.automationLog.map(item => item.key));
    state.automationLog = [...imported.filter(item => !existing.has(item.key)), ...state.automationLog];
    writeLocalState();
  } catch {}
  render();
}

async function askAssistant(prompt) {
  const chat = byId("assistant-chat");
  const pendingId = `ai-${Date.now()}`;
  chat.innerHTML += `<div class="user-bubble">${escapeHtml(prompt)}</div><div class="ai-bubble" id="${pendingId}">Thinking with CRM database context...</div>`;
  if (backendStatus.ai === "fallback") {
    byId(pendingId).textContent = buildAssistantAnswer(prompt);
    return;
  }
  try {
    const answer = await requestAI("/api/ai/assistant", prompt);
    byId(pendingId).innerHTML = escapeHtml(answer).replace(/\n/g, "<br>");
  } catch {
    byId(pendingId).textContent = buildAssistantAnswer(prompt);
  }
}

function selectedLeadId() {
  const currentInquiry = state.inquiries.find(i => i.contactId === state.selectedContactId);
  return currentInquiry?.id || currentInquiry?.no || state.selectedContactId || "LEAD";
}

function selectedContactRecord() {
  return contact(state.selectedContactId) || state.contacts[0] || {};
}

function selectedCompanyForContact(contactId) {
  const p = contact(contactId);
  return company(p.companyId) || {};
}

function openCommunicationModal() {
  const p = selectedContactRecord();
  const c = selectedCompanyForContact(p.id);
  const leadId = selectedLeadId();
  openModal("AI Communication", `
    <form class="form-grid" id="comm-form" onsubmit="event.preventDefault()">
      <input name="leadId" value="${escapeHtml(leadId)}" placeholder="Lead ID" />
      <textarea name="prompt" placeholder="Prompt">Draft a concise follow-up message for this lead with clear next action.</textarea>
      <div class="button-row">
        <button id="comm-generate-btn" class="primary" type="button" onclick="debouncedGenerateMessage()">Generate Message</button>
      </div>
      <textarea id="comm-message" name="message" placeholder="Generated message preview"></textarea>
      <input name="toEmail" value="${escapeHtml(c.email || "")}" placeholder="Email recipient" />
      <input name="toPhone" value="${escapeHtml(p.whatsapp || p.phone || "")}" placeholder="WhatsApp number (+91...)" />
      <div class="button-row">
        <button id="comm-email-btn" type="button" onclick="sendCommunicationEmail()">Send Email</button>
        <button id="comm-wa-btn" type="button" onclick="sendCommunicationWhatsApp()">Send WhatsApp</button>
      </div>
      <p id="comm-status" class="connection-note">Ready</p>
    </form>
  `);
}

function setCommunicationBusy(busy) {
  const ids = ["comm-generate-btn", "comm-email-btn", "comm-wa-btn"];
  ids.forEach(id => {
    const node = byId(id);
    if (node) node.disabled = busy;
  });
}

function setCommunicationStatus(text) {
  const node = byId("comm-status");
  if (node) node.textContent = text;
}

function debouncedGenerateMessage() {
  clearTimeout(aiGenerateDebounceTimer);
  aiGenerateDebounceTimer = setTimeout(() => generateCommunicationMessage(), 400);
}

async function generateCommunicationMessage() {
  if (aiSessionCalls >= AI_SESSION_LIMIT) {
    setCommunicationStatus("AI call limit reached for this session. Please send manually.");
    return;
  }
  const form = byId("comm-form");
  if (!form) return;
  const data = Object.fromEntries(new FormData(form));
  setCommunicationBusy(true);
  setCommunicationStatus("Generating...");
  try {
    const response = await apiRequest("/generate-message", {
      method: "POST",
      body: JSON.stringify({
        leadId: data.leadId || selectedLeadId(),
        prompt: data.prompt,
        userId: state.session?.id || "anonymous",
        state: serializableState()
      })
    });
    aiSessionCalls += 1;
    byId("comm-message").value = response.message || "";
    setCommunicationStatus(`Generated via ${response.provider}${response.cached ? " (cached)" : ""}`);
  } catch (error) {
    setCommunicationStatus(`Generation failed: ${error.message}`);
  } finally {
    setCommunicationBusy(false);
  }
}

async function sendCommunicationEmail() {
  const form = byId("comm-form");
  if (!form) return;
  const data = Object.fromEntries(new FormData(form));
  setCommunicationBusy(true);
  setCommunicationStatus("Sending email...");
  try {
    const response = await apiRequest("/send-email", {
      method: "POST",
      body: JSON.stringify({
        leadId: data.leadId || selectedLeadId(),
        to: data.toEmail,
        subject: `Follow-up for ${data.leadId || selectedLeadId()}`,
        message: data.message || ""
      })
    });
    setCommunicationStatus(`Email ${response.status} via ${response.provider}`);
  } catch (error) {
    setCommunicationStatus(`Email failed: ${error.message}`);
  } finally {
    setCommunicationBusy(false);
  }
}

async function sendCommunicationWhatsApp() {
  const form = byId("comm-form");
  if (!form) return;
  const data = Object.fromEntries(new FormData(form));
  setCommunicationBusy(true);
  setCommunicationStatus("Sending WhatsApp...");
  try {
    const response = await apiRequest("/send-whatsapp", {
      method: "POST",
      body: JSON.stringify({
        leadId: data.leadId || selectedLeadId(),
        to: data.toPhone,
        message: data.message || ""
      })
    });
    setCommunicationStatus(`WhatsApp ${response.status} via ${response.provider}`);
  } catch (error) {
    setCommunicationStatus(`WhatsApp failed: ${error.message}`);
  } finally {
    setCommunicationBusy(false);
  }
}

function askAssistantForm(event) {
  event.preventDefault();
  const prompt = new FormData(event.target).get("question");
  askAssistant(prompt);
  event.target.reset();
}

function buildAssistantAnswer(prompt) {
  const lower = String(prompt).toLowerCase();
  if (lower.includes("overdue")) return `${state.activities.filter(a => !a.done && a.due <= today()).length} follow-ups are due. Highest priority: ${state.activities.find(a => !a.done)?.title || "none"}.`;
  if (lower.includes("pipeline")) return `Current pipeline is ${money(state.pipeline.reduce((sum, d) => sum + d.value, 0))} across ${state.pipeline.length} deals. Negotiation value is ${money(state.pipeline.filter(d => d.stageId === "s-negotiation").reduce((s, d) => s + d.value, 0))}.`;
  if (lower.includes("quote")) return `Suggested follow-up: thank the buyer, restate validity, mention GST-inclusive total, and offer a quick technical call.`;
  return `I found ${state.companies.length} companies, ${state.inquiries.length} inquiries, ${state.quotations.length} quotations, and ${state.orders.length} orders in the CRM.`;
}

function globalSearch() {
  openModal("Global Search", `<form class="form-grid" onsubmit="submitGlobalSearch(event)"><input name="term" placeholder="Search companies, contacts, inquiries, quotations, orders" required /><button class="primary">Search</button></form><div id="search-results"></div>`);
}

function submitGlobalSearch(event) {
  event.preventDefault();
  const term = new FormData(event.target).get("term");
  if (!term) return;
  const haystack = [
    ...state.companies.map(c => ["Company", c.name]),
    ...state.contacts.map(p => ["Contact", `${p.first} ${p.last}`]),
    ...state.inquiries.map(i => ["Inquiry", i.no]),
    ...state.quotations.map(q => ["Quotation", q.no]),
    ...state.orders.map(o => ["Order", o.no])
  ];
  const matches = haystack.filter(([, value]) => value.toLowerCase().includes(String(term).toLowerCase()));
  byId("search-results").innerHTML = `<div class="search-results">${matches.map(([type, value]) => `<div class="settings-row"><span>${escapeHtml(value)}</span><b>${type}</b></div>`).join("") || "<p>No matches</p>"}</div>`;
}

function exportCsv(kind) {
  const rows = kind === "reports"
    ? [["Metric","Value"], ["Companies", state.companies.length], ["Inquiries", state.inquiries.length], ["Pipeline", state.pipeline.reduce((s,d)=>s+d.value,0)]]
    : [["Month","Revenue"], ["Jan", 920000], ["Feb", 1100000], ["Mar", 1260000], ["Apr", 1410000]];
  const csv = rows.map(row => row.join(",")).join("\n");
  const blob = new Blob([csv], { type: "text/csv" });
  const a = document.createElement("a");
  a.href = URL.createObjectURL(blob);
  a.download = `jkfc-${kind}.csv`;
  a.click();
}

function openCompanyDetail(id) {
  const c = company(id);
  openModal(c.name, `<div class="detail-tabs"><button>Overview</button><button>Inquiries</button><button>Contacts</button><button>Orders</button><button>Activities</button><button>Documents</button></div><div class="detail-grid"><p><b>Industry</b>${c.industry}</p><p><b>GST</b>${c.gst}</p><p><b>Owner</b>${user(c.assignedTo).name}</p><p><b>Status</b>${c.status}</p></div><h3>Timeline</h3>${state.activities.filter(a => a.companyId === id).map(activityCard).join("") || "<p>No activity yet.</p>"}`);
}

function openContactDetail(id) {
  const p = contact(id);
  openModal(`${p.first} ${p.last}`, `<div class="detail-grid"><p><b>Company</b>${company(p.companyId).name}</p><p><b>Designation</b>${p.designation}</p><p><b>Email</b>${p.email}</p><p><b>WhatsApp</b>${p.whatsapp}</p></div>`);
}

function openInquiryDetail(id) {
  const i = inquiry(id);
  const products = inquiryProducts(i);
  const totals = inquiryTotals(i);
  const canConvert = products.length > 0 && totals.total > 0;
  const locked = isActionLocked(`convert:${id}`);
  openModal(i.no, `<div class="detail-grid"><p><b>Company</b>${escapeHtml(company(i.companyId).name)}</p><p><b>Status</b>${escapeHtml(i.status)}</p><p><b>Priority</b>${escapeHtml(i.priority)}</p><p><b>Total</b>${money(totals.total || i.budgetMax)}</p></div><p>${escapeHtml(i.notes || "")}</p><h3>Products</h3><table><thead><tr><th>Item</th><th>Qty</th><th>Unit</th><th>Total</th></tr></thead><tbody>${products.map(product => `<tr><td>${escapeHtml(productLabel(product))}</td><td>${product.qty}</td><td>${money(product.unitPrice)}</td><td>${money(lineTotal(product))}</td></tr>`).join("") || "<tr><td colspan='4'>No products added yet.</td></tr>"}</tbody></table>${canWrite() ? `<button class="primary" ${canConvert && !locked ? "" : "disabled"} onclick="convertToQuote('${id}')">${locked ? "Converting..." : "Convert to Quotation"}</button>` : ""}`);
}

function selectConversation(contactId) {
  state.selectedContactId = contactId;
  writeLocalState();
  render();
}

render();
hydrateFromDatabase();
