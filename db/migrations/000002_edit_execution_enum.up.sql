-- enum execution_status
ALTER TABLE executions
ALTER COLUMN STATUS DROP DEFAULT;

CREATE TYPE execution_status_new AS ENUM (
    'finished',
    'failed',
    'totally_failed'
);

ALTER TABLE executions
ALTER COLUMN STATUS TYPE execution_status_new USING STATUS::text::execution_status_new;

DROP TYPE execution_status;

ALTER TYPE execution_status_new
RENAME TO execution_status;

-- index
CREATE INDEX idx_executions_email_latest ON executions (email_id, created_at DESC);