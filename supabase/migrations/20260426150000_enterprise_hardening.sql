-- 20260426150000_enterprise_hardening.sql

-- 1. Create Background Jobs Table
CREATE TABLE IF NOT EXISTS background_jobs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    task_type VARCHAR(50) NOT NULL, -- e.g., 'EMAIL', 'WHATSAPP'
    payload JSONB NOT NULL,
    status VARCHAR(20) DEFAULT 'PENDING',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
    processed_at TIMESTAMP WITH TIME ZONE
);

-- 2. Add Indexes for Pagination and Lookup Performance
CREATE INDEX IF NOT EXISTS idx_inquiries_created_at_desc ON inquiries (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_orders_created_at_desc ON orders (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_workspaces_members_workspace_id ON workspace_members (workspace_id);

-- 3. Enforce Strict Constraints (Idempotency / Duplication / Validity)

-- Prevent generating duplicate orders for the same quotation
ALTER TABLE orders DROP CONSTRAINT IF EXISTS unique_order_per_quotation;
ALTER TABLE orders ADD CONSTRAINT unique_order_per_quotation UNIQUE (quotation_id);

-- Enforce strictly positive amounts to prevent corrupted UI states
ALTER TABLE quotations DROP CONSTRAINT IF EXISTS check_quotation_total_positive;
ALTER TABLE quotations ADD CONSTRAINT check_quotation_total_positive CHECK (total_amount > 0);

ALTER TABLE orders DROP CONSTRAINT IF EXISTS check_order_total_positive;
ALTER TABLE orders ADD CONSTRAINT check_order_total_positive CHECK (total_amount > 0);

-- Enforce arrays to have at least one product
ALTER TABLE inquiries DROP CONSTRAINT IF EXISTS check_inquiry_products_not_empty;
ALTER TABLE inquiries ADD CONSTRAINT check_inquiry_products_not_empty CHECK (jsonb_array_length(products) > 0);
