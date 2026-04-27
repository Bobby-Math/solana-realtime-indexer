-- Migration: Create read-only user for dashboard access
-- Dashboard connects with limited permissions (SELECT only)

-- Create the dashboard reader user
-- Change the password after first deployment!
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'dashboard_reader') THEN
    CREATE ROLE dashboard_reader WITH LOGIN PASSWORD 'change_this_password_immediately';
  END IF;
END $$;

-- Grant SELECT on all existing tables
GRANT SELECT ON ALL TABLES IN SCHEMA public TO dashboard_reader;

-- Grant SELECT on future tables (automatic)
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO dashboard_reader;

-- Grant usage on all sequences (for any future serial columns)
GRANT USAGE ON ALL SEQUENCES IN SCHEMA public TO dashboard_reader;
