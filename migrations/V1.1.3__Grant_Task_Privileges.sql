-- V1.1.3__Grant_Task_Privileges.sql
-- Grant necessary privileges for task execution

-- =====================================================
-- GRANT PRIVILEGES FOR TASK EXECUTION
-- =====================================================

-- Switch to ACCOUNTADMIN role to grant privileges
USE ROLE ACCOUNTADMIN;

-- Grant EXECUTE TASK privilege to SYSADMIN role
GRANT EXECUTE TASK ON ACCOUNT TO ROLE SYSADMIN;

-- Grant EXECUTE MANAGED TASK privilege (for serverless tasks)
GRANT EXECUTE MANAGED TASK ON ACCOUNT TO ROLE SYSADMIN;

-- Grant usage on the warehouse
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE SYSADMIN;

-- Grant privileges on ANALYTICS schema
GRANT USAGE ON SCHEMA ANALYTICS TO ROLE SYSADMIN;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA ANALYTICS TO ROLE SYSADMIN;
GRANT ALL PRIVILEGES ON ALL VIEWS IN SCHEMA ANALYTICS TO ROLE SYSADMIN;
GRANT ALL PRIVILEGES ON ALL PROCEDURES IN SCHEMA ANALYTICS TO ROLE SYSADMIN;
GRANT ALL PRIVILEGES ON ALL STREAMS IN SCHEMA ANALYTICS TO ROLE SYSADMIN;

-- Grant privileges on COINDESK schema (needed for reading NEWS table)
GRANT USAGE ON SCHEMA COINDESK TO ROLE SYSADMIN;
GRANT SELECT ON ALL TABLES IN SCHEMA COINDESK TO ROLE SYSADMIN;

-- Grant privileges on NEWHEDGE schema (for completeness)
GRANT USAGE ON SCHEMA NEWHEDGE TO ROLE SYSADMIN;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA NEWHEDGE TO ROLE SYSADMIN;

-- =====================================================
-- FUTURE GRANTS (for new objects)
-- =====================================================

-- Ensure future tables/views/procedures automatically get privileges
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA ANALYTICS TO ROLE SYSADMIN;
GRANT ALL PRIVILEGES ON FUTURE VIEWS IN SCHEMA ANALYTICS TO ROLE SYSADMIN;
GRANT ALL PRIVILEGES ON FUTURE PROCEDURES IN SCHEMA ANALYTICS TO ROLE SYSADMIN;
GRANT ALL PRIVILEGES ON FUTURE STREAMS IN SCHEMA ANALYTICS TO ROLE SYSADMIN;

GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA COINDESK TO ROLE SYSADMIN;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA NEWHEDGE TO ROLE SYSADMIN;

-- =====================================================
-- SWITCH BACK TO SYSADMIN
-- =====================================================

USE ROLE SYSADMIN;

-- =====================================================
-- COMMENTS
-- =====================================================

COMMENT ON SCHEMA ANALYTICS IS 'Analytics schema with sentiment analysis. SYSADMIN has EXECUTE TASK and full privileges.';
