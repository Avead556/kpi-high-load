-- clean queue between series
TRUNCATE task_results, tasks RESTART IDENTITY;
SELECT pg_stat_reset();
