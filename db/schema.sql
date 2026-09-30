CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE incidents (
  id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id   UUID NOT NULL,
  service_id  UUID NOT NULL,
  title       TEXT NOT NULL,
  severity    TEXT NOT NULL CHECK (severity IN ('P1','P2','P3','P4')),
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE idempotency_keys (
  id              BIGSERIAL PRIMARY KEY,
  tenant_id       UUID NOT NULL,
  operation       TEXT NOT NULL,
  key             TEXT NOT NULL,
  request_hash    TEXT NOT NULL,
  state           TEXT NOT NULL CHECK (state IN ('processing', 'completed', 'failed')),
  incident_id     UUID REFERENCES incidents(id) ON DELETE SET NULL,
  response_status INTEGER,
  response_body   JSONB,
  response_headers JSONB,
  expires_at      TIMESTAMPTZ NOT NULL,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

  CONSTRAINT uq_idempotency_scope
    UNIQUE (tenant_id, operation, key)
);

CREATE INDEX idx_idempotency_expiry
  ON idempotency_keys (expires_at);

CREATE TABLE paging_jobs (
  id          BIGSERIAL PRIMARY KEY,
  incident_id UUID NOT NULL REFERENCES incidents(id) ON DELETE CASCADE,
  tenant_id   UUID NOT NULL,
  status      TEXT NOT NULL DEFAULT 'pending',
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),

  CONSTRAINT uq_paging_job_incident
    UNIQUE (incident_id)
);

CREATE INDEX idx_paging_jobs_status
  ON paging_jobs (status);
