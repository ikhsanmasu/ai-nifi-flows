# Required Registry Client
1. Registry Clients - GitHubFlowRegistryClient 2.6.0	
    - GitHub API URL: https://api.github.com/
    - Repository Owner: ikhsamasu
    - Repository Name: ai-nifi-flow
    - Authentication Type: Personal Access Token
    - Personal Access Token: github_pat_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
    - Default Branch: ikhsan/dev
    - Repository Path: flows
    

# Required Services (Example)
1. DBCPConnectionPool - PostgreSQL
    - Database Connection URL: jdbc:postgresql://host.docker.internal:5434/maxmar
    - Database Driver Class Name: org.postgresql.Driver
    - Database Driver Locations(s): /opt/nifi/nifi-current/lib/jdbc/postgresql-42.7.3.jar
    - Database User: postgres
    - Password: postgres
    - Validation Query: SELECT 1



