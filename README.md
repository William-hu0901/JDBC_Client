# JDBC_Client
This is a Client to connect with different databases such as MySQL, PostgreSQL, Oracle, MongoDB, and Neo4j with given parameters.

## PostgreSQL Connection Example
```java
PostgresConnector postgresConnector = new PostgresConnector(
    "database-postgres.chams8ws6974.ap-southeast-1.rds.amazonaws.com",
    5432,
    "postgres",
    "postgres",
    "123"
);
postgresConnector.connect();
```

## Supported Databases
- PostgreSQL
- MySQL
- Oracle
- MongoDB
- Neo4j

## Features
- Simple CRUD operations for each database.
- Custom exception handling for each database.
- Logging with SLF4J and Logback.
