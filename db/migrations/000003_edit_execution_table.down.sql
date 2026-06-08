DROP INDEX IF EXISTS idx_executions_email_latest;

CREATE INDEX idx_executions_email_latest ON executions (email_id, created_at DESC);

ALTER TABLE executions DROP COLUMN IF EXISTS executed_at;