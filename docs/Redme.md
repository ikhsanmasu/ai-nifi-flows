# postgres note
## sesuaikan setting seperti contoh dibawah

-- Jalankan ini di PostgreSQL existing:
docker exec -it <container_name> psql -U postgres

-- 1. Buat user
CREATE USER debezium WITH REPLICATION LOGIN PASSWORD 'debezium';

-- 2. Grant permissions
GRANT CONNECT ON DATABASE maxmar TO debezium;
GRANT USAGE ON SCHEMA cultivation TO debezium;
GRANT SELECT ON ALL TABLES IN SCHEMA cultivation TO debezium;
ALTER DEFAULT PRIVILEGES IN SCHEMA cultivation GRANT SELECT ON TABLES TO debezium;

-- 3. Buat publication
CREATE PUBLICATION debezium_pub FOR ALL TABLES;

-- 4. Verify
SHOW wal_level;  -- harus 'logical'
SELECT * FROM pg_publication;

-- 5 Sesuaikan setting
ALTER SYSTEM SET wal_level = 'logical';
ALTER SYSTEM SET max_replication_slots = 4;
ALTER SYSTEM SET max_wal_senders = 4;


docker restart <container_name>
----------------------------------------------------------------------------------
# debezium note

sesuaikan debezium-register-connector.json

lalu register
curl -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d @debezium-register-connector.json

cek connector status
curl -s http://localhost:8083/connectors/postgres-connector/status

Cek replication slot di PostgreSQL
docker exec -it postgres-maxmar psql -U postgres -d maxmar -c "SELECT slot_name, active FROM pg_replication_slots;"
----------------------------------------------------------------------------------
# Nifi note

## Required Registry Client (Example)
1. Registry Clients - GitHubFlowRegistryClient 2.6.0	
    - GitHub API URL: https://api.github.com/
    - Repository Owner: ikhsamasu
    - Repository Name: ai-nifi-flows
    - Authentication Type: Personal Access Token
    - Personal Access Token: xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
    - Default Branch: ikhsan/dev
    - Repository Path: flows
    

## Required Services (Example)
1. DBCPConnectionPool - PostgreSQL
    - Database Connection URL: jdbc:postgresql://host.docker.internal:5434/maxmar
    - Database Driver Class Name: org.postgresql.Driver
    - Database Driver Locations(s): /opt/nifi/nifi-current/lib/jdbc/postgresql-42.7.3.jar
    - Database User: postgres
    - Password: postgres
    - Validation Query: SELECT 1

2. DBCPConnectionPool - Clickhouse
    - Database Connection URL: jdbc:ch://host.docker.internal:8123
    - Database Driver Class Name: com.clickhouse.jdbc.ClickHouseDriver
    - Database Driver Location(s): /opt/nifi/nifi-current/lib/jdbc/clickhouse-jdbc-0.6.5-all.jar
    - Database User: default
    - Password: (kosong atau password kamu)
    - Validation Query: SELECT 1


