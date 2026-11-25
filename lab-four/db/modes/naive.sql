-- step 3: task_results keeps duplicates, the primary key is replaced by a plain index
ALTER TABLE task_results DROP CONSTRAINT IF EXISTS task_results_pkey;
CREATE INDEX IF NOT EXISTS task_results_task_id_idx ON task_results (task_id);
