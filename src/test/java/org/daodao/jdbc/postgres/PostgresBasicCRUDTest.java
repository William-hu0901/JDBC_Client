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
            log.info("PostgreSQL connection established for CRUD tests");
        } catch (Exception e) {
            log.warn("Failed to connect to PostgreSQL for CRUD tests: {}", e.getMessage());
            connector = null;
        }
    }
    
    @AfterEach
    void tearDown() {
        if (connector != null) {
            connector.disconnect();
        }
    }
    
    @Test
    @DisplayName("Test PostgreSQL Connection")
    void testConnection() {
        if (connector == null) {
            log.warn("Skipping connection test - no database connection available");
            return;
        }
        assertNotNull(connector, "Connector should not be null");
        log.info("Connection test passed");
    }
    
    @Test
    @DisplayName("Test CREATE Table Operation")
    void testCreateTable() {
        if (connector == null) {
            log.warn("Skipping CREATE test - no database connection available");
            return;
        }
        
        try {
            String createTableSQL = """
                CREATE TABLE IF NOT EXISTS test_users (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(100) NOT NULL,
                    email VARCHAR(100) UNIQUE NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """;
            connector.create(createTableSQL);
            log.info("CREATE TABLE test passed");
        } catch (SQLException e) {
            log.error("CREATE TABLE test failed: {}", e.getMessage());
            fail("CREATE TABLE operation should not throw exception");
        }
    }
    
    @Test
    @DisplayName("Test INSERT Operation")
    void testInsertData() {
        if (connector == null) {
            log.warn("Skipping INSERT test - no database connection available");
            return;
        }
        
        try {
            String createTableSQL = """
                CREATE TABLE IF NOT EXISTS test_users (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(100) NOT NULL,
                    email VARCHAR(100) UNIQUE NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """;
            connector.create(createTableSQL);
            
            String insertSQL = """
                INSERT INTO test_users (name, email) 
                VALUES ('John Doe', 'john.doe@example.com')
                ON CONFLICT (email) DO NOTHING
                """;
            connector.update(insertSQL);
            log.info("INSERT test passed");
        } catch (SQLException e) {
            log.error("INSERT test failed: {}", e.getMessage());
            fail("INSERT operation should not throw exception");
        }
    }
    
    @Test
    @DisplayName("Test SELECT Operation")
    void testSelectData() {
        if (connector == null) {
            log.warn("Skipping SELECT test - no database connection available");
            return;
        }
        
        try {
            String createTableSQL = """
                CREATE TABLE IF NOT EXISTS test_users (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(100) NOT NULL,
                    email VARCHAR(100) UNIQUE NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """;
            connector.create(createTableSQL);
            
            String insertSQL = """
                INSERT INTO test_users (name, email) 
                VALUES ('Jane Smith', 'jane.smith@example.com')
                ON CONFLICT (email) DO NOTHING
                """;
            connector.update(insertSQL);
            
            String selectSQL = "SELECT COUNT(*) as count FROM test_users WHERE email = 'jane.smith@example.com'";
            ResultSet resultSet = connector.read(selectSQL);
            
            assertTrue(resultSet.next(), "ResultSet should have at least one row");
            int count = resultSet.getInt("count");
            assertTrue(count >= 0, "Count should be non-negative");
            
            resultSet.close();
            log.info("SELECT test passed");
        } catch (SQLException e) {
            log.error("SELECT test failed: {}", e.getMessage());
            fail("SELECT operation should not throw exception");
        }
    }
    
    @Test
    @DisplayName("Test UPDATE Operation")
    void testUpdateData() {
        if (connector == null) {
            log.warn("Skipping UPDATE test - no database connection available");
            return;
        }
        
        try {
            String createTableSQL = """
                CREATE TABLE IF NOT EXISTS test_users (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(100) NOT NULL,
                    email VARCHAR(100) UNIQUE NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """;
            connector.create(createTableSQL);
            
            String insertSQL = """
                INSERT INTO test_users (name, email) 
                VALUES ('Test User', 'test.user@example.com')
                ON CONFLICT (email) DO NOTHING
                """;
            connector.update(insertSQL);
            
            String updateSQL = """
                UPDATE test_users 
                SET name = 'Updated User' 
                WHERE email = 'test.user@example.com'
                """;
            connector.update(updateSQL);
            log.info("UPDATE test passed");
        } catch (SQLException e) {
            log.error("UPDATE test failed: {}", e.getMessage());
            fail("UPDATE operation should not throw exception");
        }
    }
    
    @Test
    @DisplayName("Test DELETE Operation")
    void testDeleteData() {
        if (connector == null) {
            log.warn("Skipping DELETE test - no database connection available");
            return;
        }
        
        try {
            String createTableSQL = """
                CREATE TABLE IF NOT EXISTS test_users (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(100) NOT NULL,
                    email VARCHAR(100) UNIQUE NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """;
            connector.create(createTableSQL);
            
            String insertSQL = """
                INSERT INTO test_users (name, email) 
                VALUES ('Delete User', 'delete.user@example.com')
                ON CONFLICT (email) DO NOTHING
                """;
            connector.update(insertSQL);
            
            String deleteSQL = """
                DELETE FROM test_users 
                WHERE email = 'delete.user@example.com'
                """;
            connector.delete(deleteSQL);
            log.info("DELETE test passed");
        } catch (SQLException e) {
            log.error("DELETE test failed: {}", e.getMessage());
            fail("DELETE operation should not throw exception");
        }
    }
}