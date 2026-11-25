-- step 4 onwards: one result per task again
DELETE FROM task_results;
DROP INDEX IF EXISTS task_results_task_id_idx;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'task_results_pkey') THEN
        ALTER TABLE task_results ADD CONSTRAINT task_results_pkey PRIMARY KEY (task_id);
    END IF;
END $$;
