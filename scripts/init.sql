CREATE TABLE IF NOT EXISTS logs (
  id UUID PRIMARY KEY,
  organization_id VARCHAR(255) NOT NULL,
  level VARCHAR(50) NOT NULL,
  message TEXT NOT NULL,
  source VARCHAR(255) NOT NULL,
  timestamp TIMESTAMPTZ NOT NULL,
  processed_at TIMESTAMPTZ NOT NULL
);

-- Optimze queries for specific tenant and time ranges
CREATE INDEX idx_org_time ON logs(organization_id, timestamp DESC);