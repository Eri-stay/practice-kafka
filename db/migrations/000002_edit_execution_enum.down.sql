CREATE TYPE execution_status_old AS ENUM (
    'in_progress',
    'finished',
    'failed'
);

ALTER TABLE executions
ALTER COLUMN STATUS TYPE execution_status_old USING STATUS::text::execution_status_old;

DROP TYPE execution_status;

ALTER TYPE execution_status_old
RENAME TO execution_status;

ALTER TABLE executions
ALTER COLUMN STATUS
SET DEFAULT 'in_progress';

-- index
DROP INDEX IF EXISTS idx_executions_email_latest;