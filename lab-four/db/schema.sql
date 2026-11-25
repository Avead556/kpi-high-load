CREATE TABLE IF NOT EXISTS vaccine_lots (
    id                 BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    lot_number         VARCHAR(32)  NOT NULL,
    vaccine            VARCHAR(100) NOT NULL,
    doses              INTEGER      NOT NULL,
    temperature_regime VARCHAR(16)  NOT NULL,
    manufactured_on    DATE         NOT NULL,
    expires_on         DATE         NOT NULL,
    created_at         TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT vaccine_lots_lot_number_key UNIQUE (lot_number),
    CONSTRAINT vaccine_lots_doses_positive CHECK (doses > 0),
    CONSTRAINT vaccine_lots_expiry_after_manufacture CHECK (expires_on > manufactured_on),
    CONSTRAINT vaccine_lots_temperature_regime CHECK (temperature_regime IN ('+2..+8', '-25..-15', '-90..-60'))
);

CREATE INDEX IF NOT EXISTS vaccine_lots_vaccine_id_idx ON vaccine_lots (vaccine, id);

CREATE TABLE IF NOT EXISTS tasks (
    id                  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    record_id           BIGINT      NOT NULL REFERENCES vaccine_lots (id) ON DELETE CASCADE,
    status              VARCHAR(16) NOT NULL DEFAULT 'queued',
    seed                BIGINT      NOT NULL,
    attempts            INTEGER     NOT NULL DEFAULT 0,
    max_attempts        INTEGER     NOT NULL DEFAULT 3,
    token               BIGINT      NOT NULL DEFAULT 0,
    lease_until         TIMESTAMPTZ,
    worker_id           TEXT,
    cancel_requested_at TIMESTAMPTZ,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    started_at          TIMESTAMPTZ,
    finished_at         TIMESTAMPTZ,
    CONSTRAINT tasks_status CHECK (status IN ('queued', 'running', 'done', 'failed', 'cancelled')),
    CONSTRAINT tasks_seed_range CHECK (seed >= 0 AND seed < 2147483648)
);

CREATE INDEX IF NOT EXISTS tasks_status_created_at_idx ON tasks (status, created_at);
CREATE INDEX IF NOT EXISTS tasks_record_id_idx ON tasks (record_id);

CREATE TABLE IF NOT EXISTS task_results (
    task_id    BIGINT      NOT NULL REFERENCES tasks (id) ON DELETE CASCADE,
    token      BIGINT      NOT NULL,
    worker_id  TEXT        NOT NULL,
    result     JSONB       NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT task_results_pkey PRIMARY KEY (task_id)
);
