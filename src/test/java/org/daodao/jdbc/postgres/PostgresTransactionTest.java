package org.daodao.jdbc.postgres;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import static org.junit.jupiter.api.Assertions.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.daodao.jdbc.connectors.PostgresConnector;
import org.daodao.jdbc.config.PostgresConfig;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

public class PostgresTransactionTest {
    
    private static final Logger log = LoggerFactory.getLogger(PostgresTransactionTest.class);
    private PostgresConnector connector;
    private PostgresConfig config;
    private Connection directConnection;
    
    @BeforeEach
    void setUp() {
        try {
            config = new PostgresConfig();
            connector = new PostgresConnector(
                config.getPostgresHost(),
                config.getPostgresPort(),
                config.getPostgresDatabase(),
                config.getPostgresUsername(),
                config.getPostgresPassword()
            );
            connector.connect();
            
            // Create direct connection for transaction testing
            Properties props = new Properties();
            props.setProperty("user", config.getPostgresUsername());
            props.setProperty("password", config.getPostgresPassword());
            String url = String.format("jdbc:postgresql://%s:%d/%s?sslmode=prefer", 
                config.getPostgresHost(), config.getPostgresPort(), config.getPostgresDatabase());
            directConnection = DriverManager.getConnection(url, props);
            
            log.info("PostgreSQL connections established for transaction tests");
        } catch (Exception e) {
            log.warn("Failed to connect to PostgreSQL for transaction tests: {}", e.getMessage());
            connector = null;
            directConnection = null;
        }
    }
    
    @AfterEach
    void tearDown() {
        try {
            if (directConnection != null && !directConnection.isClosed()) {
                directConnection.close();
            }
        } catch (SQLException e) {
            log.error("Error closing direct connection: {}", e.getMessage());
        }
        
        if (connector != null) {
            connector.disconnect();
        }
    }
    
    @Test
    @DisplayName("Test Transaction Commit")
    void testTransactionCommit() {
        if (directConnection == null) {
            log.warn("Skipping transaction commit test - no database connection available");
            return;
        }
        
        try {
            String createTableSQL = """
                CREATE TABLE IF NOT EXISTS transaction_test (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(100),
                    amount NUMERIC(10,2)
                )
                """;
            connector.create(createTableSQL);
            
            try (var statement = directConnection.createStatement()) {
                statement.execute("BEGIN");
                log.info("Transaction started");
                
                statement.executeUpdate("INSERT INTO transaction_test (name, amount) VALUES ('User1', 100.00)");
                statement.executeUpdate("INSERT INTO transaction_test (name, amount) VALUES ('User2', 200.00)");
                
                statement.execute("COMMIT");
                log.info("Transaction committed");
                
                try (var resultSet = statement.executeQuery("SELECT COUNT(*) as count FROM transaction_test WHERE name IN ('User1', 'User2')")) {
                    assertTrue(resultSet.next());
                    int count = resultSet.getInt("count");
                    assertEquals(2, count, "Both records should be committed");
                }
            }
            
            log.info("Transaction commit test passed");
        } catch (SQLException e) {
            log.error("Transaction commit test failed: {}", e.getMessage());
            fail("Transaction commit should not throw exception");
        }
    }
    
    @Test
    @DisplayName("Test Transaction Rollback")
    void testTransactionRollback() {
        if (directConnection == null) {
            log.warn("Skipping transaction rollback test - no database connection available");
            return;
        }
        
        try {
            String createTableSQL = """
                CREATE TABLE IF NOT EXISTS transaction_rollback_test (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(100),
                    temp_data VARCHAR(100)
                )
                """;
            connector.create(createTableSQL);
            
            try (var statement = directConnection.createStatement()) {
                statement.execute("BEGIN");
                log.info("Transaction started for rollback test");
                
                statement.executeUpdate("INSERT INTO transaction_rollback_test (name, temp_data) VALUES ('TempUser', 'temp_data')");
                
                statement.execute("ROLLBACK");
                log.info("Transaction rolled back");
                
                try (var resultSet = statement.executeQuery("SELECT COUNT(*) as count FROM transaction_rollback_test WHERE name = 'TempUser'")) {
                    assertTrue(resultSet.next());
                    int count = resultSet.getInt("count");
                    assertEquals(0, count, "No records should exist after rollback");
                }
            }
            
            log.info("Transaction rollback test passed");
        } catch (SQLException e) {
            log.error("Transaction rollback test failed: {}", e.getMessage());
            fail("Transaction rollback should not throw exception");
        }
    }
    
    @Test
    @DisplayName("Test Savepoint")
    void testSavepoint() {
        if (directConnection == null) {
            log.warn("Skipping savepoint test - no database connection available");
            return;
        }
        
        try {
            String createTableSQL = """
                CREATE TABLE IF NOT EXISTS savepoint_test (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(100),
                    value INTEGER
                )
                """;
            connector.create(createTableSQL);
            
            try (var statement = directConnection.createStatement()) {
                statement.execute("BEGIN");
                log.info("Transaction started for savepoint test");
                
                statement.executeUpdate("INSERT INTO savepoint_test (name, value) VALUES ('Record1', 10)");
                
                statement.execute("SAVEPOINT sp1");
                log.info("Savepoint sp1 created");
                
                statement.executeUpdate("INSERT INTO savepoint_test (name, value) VALUES ('Record2', 20)");
                statement.executeUpdate("INSERT INTO savepoint_test (name, value) VALUES ('Record3', 30)");
                
                statement.execute("ROLLBACK TO SAVEPOINT sp1");
                log.info("Rolled back to savepoint sp1");
                
                statement.execute("COMMIT");
                log.info("Transaction committed");
                
                try (var resultSet = statement.executeQuery("SELECT COUNT(*) as count FROM savepoint_test WHERE name IN ('Record1', 'Record2', 'Record3')")) {
                    assertTrue(resultSet.next());
                    int count = resultSet.getInt("count");
                    assertEquals(1, count, "Only Record1 should exist after rollback to savepoint");
                }
            }
            
            log.info("Savepoint test passed");
        } catch (SQLException e) {
            log.error("Savepoint test failed: {}", e.getMessage());
            fail("Savepoint operation should not throw exception");
        }
    }
    
    @Test
    @DisplayName("Test Transaction Isolation Level")
    void testTransactionIsolationLevel() {
        if (directConnection == null) {
            log.warn("Skipping isolation level test - no database connection available");
            return;
        }
        
        try {
            String createTableSQL = """
                CREATE TABLE IF NOT EXISTS isolation_test (
                    id SERIAL PRIMARY KEY,
                    data VARCHAR(100),
                    counter INTEGER DEFAULT 0
                )
                """;
            connector.create(createTableSQL);
            
            try (var statement = directConnection.createStatement()) {
                statement.execute("BEGIN");
                log.info("Transaction started for isolation level test");
                
                statement.executeUpdate("SET TRANSACTION ISOLATION LEVEL READ COMMITTED");
                log.info("Transaction isolation level set to READ COMMITTED");
                
                statement.executeUpdate("INSERT INTO isolation_test (data, counter) VALUES ('Test', 1)");
                
                try (var resultSet = statement.executeQuery("SELECT COUNT(*) as count FROM isolation_test WHERE data = 'Test'")) {
                    assertTrue(resultSet.next());
                    int count = resultSet.getInt("count");
                    assertEquals(1, count, "Record should be visible in same transaction");
                }
                
                statement.execute("COMMIT");
                log.info("Transaction committed");
            }
            
            log.info("Transaction isolation level test passed");
        } catch (SQLException e) {
            log.error("Transaction isolation level test failed: {}", e.getMessage());
            fail("Transaction isolation level test should not throw exception");
        }
    }
}