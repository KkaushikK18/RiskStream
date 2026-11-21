-- Least-privilege roles for RiskStream. Run as a superuser with psql variables:
--   psql -U postgres -v owner_password='...' -v app_password='...' -f infrastructure/postgres/roles.sql
--
-- riskstream_owner : owns the databases and runs migrations (DDL)
-- riskstream_app   : what the API uses. SELECT/INSERT/UPDATE only: no DDL,
--                    no DELETE, no TRUNCATE. Labels and predictions cannot be
--                    deleted by the application, even if it is compromised.

SELECT format('CREATE ROLE riskstream_owner LOGIN PASSWORD %L', :'owner_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'riskstream_owner') \gexec
SELECT format('CREATE ROLE riskstream_app LOGIN PASSWORD %L', :'app_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'riskstream_app') \gexec

SELECT 'CREATE DATABASE riskstream OWNER riskstream_owner'
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'riskstream') \gexec
SELECT 'CREATE DATABASE riskstream_test OWNER riskstream_owner'
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'riskstream_test') \gexec

\connect riskstream
REVOKE ALL ON DATABASE riskstream FROM PUBLIC;
GRANT CONNECT ON DATABASE riskstream TO riskstream_app;
GRANT USAGE ON SCHEMA public TO riskstream_app;
ALTER DEFAULT PRIVILEGES FOR ROLE riskstream_owner IN SCHEMA public
    GRANT SELECT, INSERT, UPDATE ON TABLES TO riskstream_app;
ALTER DEFAULT PRIVILEGES FOR ROLE riskstream_owner IN SCHEMA public
    GRANT USAGE, SELECT ON SEQUENCES TO riskstream_app;
-- Tables that already exist (if migrations ran before this script):
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA public TO riskstream_app;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO riskstream_app;
