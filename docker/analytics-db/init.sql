-- Create analytics database and user
CREATE DATABASE analytics_db;
CREATE USER analytics_user WITH PASSWORD 'analytics_pass';

-- Grant connection privileges
GRANT CONNECT ON DATABASE analytics_db TO analytics_user;
GRANT ALL PRIVILEGES ON DATABASE analytics_db TO analytics_user;

-- Connect to the analytics_db
\c analytics_db

-- Ensure the public schema exists
CREATE SCHEMA IF NOT EXISTS public;
ALTER SCHEMA public OWNER TO analytics_user;

-- Grant necessary permissions
GRANT ALL ON SCHEMA public TO analytics_user;
GRANT ALL ON ALL TABLES IN SCHEMA public TO analytics_user;
GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO analytics_user;

-- Modify default privileges
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO analytics_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO analytics_user;
