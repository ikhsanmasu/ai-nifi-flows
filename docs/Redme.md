-------------------------------------------------------------------------------------------------------------------------
# postgres note

## Edit postgresql.conf
Lokasi biasanya: /var/lib/postgresql/data/postgresql.conf

wal_level = logical
max_replication_slots = 4
max_wal_senders = 4

-------------------------------------------------------------------------------------------------------------------------
# debezium note

sesuaikan debezium-register-connector.json

lalu register
curl -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d @debezium-register-connector.json

cek connector status
curl -s http://localhost:8083/connectors/postgres-connector/status

Cek replication slot di PostgreSQL
docker exec -it postgres-maxmar psql -U postgres -d maxmar -c "SELECT slot_name, active FROM pg_replication_slots;"


## atau kalo pake docker
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
# Nifi note

## Required Registry Client
1. Registry Clients - GitHubFlowRegistryClient 2.6.0	
    - GitHub API URL: https://api.github.com/
    - Repository Owner: ikhsamasu
    - Repository Name: ai-nifi-flows
    - Authentication Type: Personal Access Token
    - Personal Access Token: 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
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


