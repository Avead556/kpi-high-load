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
