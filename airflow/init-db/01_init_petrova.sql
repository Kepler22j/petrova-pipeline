-- ============================================================
-- PETROVA – PostgreSQL Local Init (for dbt testing)
-- Creates: petrova database + bronze/silver/gold/public schemas
-- ============================================================

-- Create the petrova database (run as superuser)
SELECT 'CREATE DATABASE petrova'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'petrova');
\gexec

\c petrova

-- Create petrova user
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'petrova') THEN
        CREATE ROLE petrova WITH LOGIN PASSWORD 'petrova';
    END IF;
END $$;

-- Create medallion schemas
CREATE SCHEMA IF NOT EXISTS public;
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- Grant permissions
GRANT ALL PRIVILEGES ON DATABASE petrova TO petrova;
GRANT ALL PRIVILEGES ON SCHEMA public, bronze, silver, gold TO petrova;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO petrova;
ALTER DEFAULT PRIVILEGES IN SCHEMA bronze GRANT ALL ON TABLES TO petrova;
ALTER DEFAULT PRIVILEGES IN SCHEMA silver GRANT ALL ON TABLES TO petrova;
ALTER DEFAULT PRIVILEGES IN SCHEMA gold   GRANT ALL ON TABLES TO petrova;
