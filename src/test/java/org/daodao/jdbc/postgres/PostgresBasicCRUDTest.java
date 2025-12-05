package org.daodao.jdbc.postgres;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.daodao.jdbc.connectors.PostgresConnector;
import org.daodao.jdbc.config.PostgresConfig;
import org.daodao.jdbc.exceptions.PostgresException;

import java.sql.ResultSet;
import java.sql.SQLException;

public class PostgresBasicCRUDTest {
    
    private static final Logger log = LoggerFactory.getLogger(PostgresBasicCRUDTest.class);
    private PostgresConnector connector;
    private PostgresConfig config;
    
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
            
            // Clean up any existing test table before tests
            try {
                connector.update("DROP TABLE IF EXISTS test_users CASCADE");
            } catch (SQLException e) {
                log.debug("Table test_users does not exist or cannot be dropped: {}", e.getMessage());
            }
            
            log.info("PostgreSQL connection established for CRUD tests");
        } catch (Exception e) {
            log.warn("Failed to connect to PostgreSQL for CRUD tests: {}", e.getMessage());
            connector = null;
            // Skip tests if PostgreSQL is not available
            assumeTrue(false, "PostgreSQL not available: " + e.getMessage());
        }
    }
    
    @AfterEach
    void tearDown() {
        // Clean up test data
        if (connector != null) {
            try {
                connector.update("DROP TABLE IF EXISTS test_users CASCADE");
            } catch (SQLException e) {
                log.debug("Failed to clean up test table: {}", e.getMessage());
            } finally {
                connector.disconnect();
            }
        }
    }
    
    @Test
    @DisplayName("Test PostgreSQL Connection")
    // Test case for verifying PostgreSQL database connection establishment
    void testConnection() {
        assertNotNull(connector, "Connector should not be null");
        assertTrue(connector != null, "PostgreSQL connection should be established");
        log.info("Connection test passed");
    }
    
    @Test
    @DisplayName("Test CREATE Table Operation")
    // Test case for verifying PostgreSQL table creation functionality
    void testCreateTable() {
        try {
            String createTableSQL = """
                CREATE TABLE test_users (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(100) NOT NULL,
                    email VARCHAR(100) UNIQUE NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """;
            connector.create(createTableSQL);
            
            // Verify table was created by checking if we can query it
            boolean tableExists = connector.readBoolean("SELECT COUNT(*) FROM test_users");
            assertTrue(tableExists, "Should be able to query the created table");
            
            log.info("CREATE TABLE test passed");
        } catch (SQLException e) {
            log.error("CREATE TABLE test failed: {}", e.getMessage());
            fail("CREATE TABLE operation should not throw exception: " + e.getMessage());
        }
    }
    
    @Test
    @DisplayName("Test INSERT Operation")
    // Test case for verifying PostgreSQL data insertion functionality
    void testInsertData() {
        try {
            // Create table first
            String createTableSQL = """
                CREATE TABLE test_users (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(100) NOT NULL,
                    email VARCHAR(100) UNIQUE NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """;
            connector.create(createTableSQL);
            
            // Insert test data
            String insertSQL = """
                INSERT INTO test_users (name, email) 
                VALUES ('John Doe', 'john.doe@example.com')
                """;
            connector.update(insertSQL);
            
            // Verify data was inserted
            int count = connector.readSingleValue("SELECT COUNT(*) FROM test_users WHERE email = 'john.doe@example.com'");
            assertEquals(1, count, "Should find exactly one inserted record");
            
            log.info("INSERT test passed");
        } catch (SQLException e) {
            log.error("INSERT test failed: {}", e.getMessage());
            fail("INSERT operation should not throw exception: " + e.getMessage());
        }
    }
    
    @Test
    @DisplayName("Test SELECT Operation")
    // Test case for verifying PostgreSQL data retrieval functionality
    void testSelectData() {
        try {
            // Create table first
            String createTableSQL = """
                CREATE TABLE test_users (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(100) NOT NULL,
                    email VARCHAR(100) UNIQUE NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """;
            connector.create(createTableSQL);
            
            // Insert test data
            String insertSQL = """
                INSERT INTO test_users (name, email) 
                VALUES ('Jane Smith', 'jane.smith@example.com')
                """;
            connector.update(insertSQL);
            
            // Query the data
            int count = connector.readSingleValue("SELECT COUNT(*) FROM test_users WHERE email = 'jane.smith@example.com'");
            assertEquals(1, count, "Should find exactly one record for Jane Smith");
            log.info("SELECT test passed");
        } catch (SQLException e) {
            log.error("SELECT test failed: {}", e.getMessage());
            fail("SELECT operation should not throw exception: " + e.getMessage());
        }
    }
    
    @Test
    @DisplayName("Test UPDATE Operation")
    // Test case for verifying PostgreSQL data update functionality
    void testUpdateData() {
        try {
            // Create table first
            String createTableSQL = """
                CREATE TABLE test_users (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(100) NOT NULL,
                    email VARCHAR(100) UNIQUE NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """;
            connector.create(createTableSQL);
            
            // Insert test data
            String insertSQL = """
                INSERT INTO test_users (name, email) 
                VALUES ('Test User', 'test.user@example.com')
                """;
            connector.update(insertSQL);
            
            // Update the data
            String updateSQL = """
                UPDATE test_users 
                SET name = 'Updated User' 
                WHERE email = 'test.user@example.com'
                """;
            connector.update(updateSQL);
            
            // Verify the update using original read method for string values
            try (ResultSet resultSet = connector.read("SELECT name FROM test_users WHERE email = 'test.user@example.com'")) {
                assertTrue(resultSet.next(), "ResultSet should have at least one row");
                assertEquals("Updated User", resultSet.getString("name"), "Name should be updated");
            }
            
            log.info("UPDATE test passed");
        } catch (SQLException e) {
            log.error("UPDATE test failed: {}", e.getMessage());
            fail("UPDATE operation should not throw exception: " + e.getMessage());
        }
    }
    
    @Test
    @DisplayName("Test DELETE Operation")
    // Test case for verifying PostgreSQL data deletion functionality
    void testDeleteData() {
        try {
            // Create table first
            String createTableSQL = """
                CREATE TABLE test_users (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(100) NOT NULL,
                    email VARCHAR(100) UNIQUE NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """;
            connector.create(createTableSQL);
            
            // Insert test data
            String insertSQL = """
                INSERT INTO test_users (name, email) 
                VALUES ('Delete User', 'delete.user@example.com')
                """;
            connector.update(insertSQL);
            
            // Verify data exists before deletion
            int beforeCount = connector.readSingleValue("SELECT COUNT(*) FROM test_users WHERE email = 'delete.user@example.com'");
            assertEquals(1, beforeCount, "Should find exactly one record before deletion");
            
            // Delete the data
            String deleteSQL = """
                DELETE FROM test_users 
                WHERE email = 'delete.user@example.com'
                """;
            connector.delete(deleteSQL);
            
            // Verify the deletion
            int afterCount = connector.readSingleValue("SELECT COUNT(*) FROM test_users WHERE email = 'delete.user@example.com'");
            assertEquals(0, afterCount, "Should find no records after deletion");
            
            log.info("DELETE test passed");
        } catch (SQLException e) {
            log.error("DELETE test failed: {}", e.getMessage());
            fail("DELETE operation should not throw exception: " + e.getMessage());
        }
    }
}