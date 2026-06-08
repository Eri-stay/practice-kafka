ALTER TABLE executions
ADD COLUMN executed_at TIMESTAMPTZ NOT NULL DEFAULT NOW();

DROP INDEX IF EXISTS idx_executions_email_latest;

CREATE INDEX idx_executions_email_latest ON executions (email_id, executed_at DESC);