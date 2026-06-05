CREATE TYPE email_status AS ENUM (
    'pending',
    'in_progress',
    'sent',
    'failed',
    'totally_failed'
);

CREATE TYPE execution_status AS ENUM (
    'in_progress',
    'finished',
    'failed'
);

CREATE TABLE IF NOT EXISTS emails (
    id SERIAL PRIMARY KEY,
    recipient VARCHAR(255) NOT NULL,
    subject VARCHAR(255) NOT NULL,
    body TEXT NOT NULL,
    schedule_time TIMESTAMPTZ,
    STATUS email_status NOT NULL DEFAULT 'pending',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    sent_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_emails_status ON emails(STATUS, schedule_time);

CREATE INDEX IF NOT EXISTS idx_emails_created_at ON emails(created_at);

CREATE TABLE IF NOT EXISTS executions (
    id SERIAL PRIMARY KEY,
    email_id INTEGER NOT NULL REFERENCES emails(id) ON DELETE CASCADE,
    STATUS execution_status NOT NULL DEFAULT 'in_progress',
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_executions_status ON executions(STATUS);

CREATE INDEX IF NOT EXISTS idx_executions_created_at ON executions(created_at);