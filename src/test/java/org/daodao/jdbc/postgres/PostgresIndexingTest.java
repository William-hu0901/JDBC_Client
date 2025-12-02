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

public class PostgresIndexingTest {
    
    private static final Logger log = LoggerFactory.getLogger(PostgresIndexingTest.class);
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
            log.info("PostgreSQL connection established for indexing tests");
        } catch (Exception e) {
            log.warn("Failed to connect to PostgreSQL for indexing tests: {}", e.getMessage());
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
    @DisplayName("Test Create B-Tree Index")
    void testCreateBTreeIndex() {
        if (connector == null) {
            log.warn("Skipping B-Tree index test - no database connection available");
            return;
        }
        
        try {
            String createTableSQL = """
                CREATE TABLE IF NOT EXISTS index_test (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(100),
                    email VARCHAR(100),
                    age INTEGER
                )
                """;
            connector.create(createTableSQL);
            
            String createIndexSQL = """
                CREATE INDEX IF NOT EXISTS idx_index_test_email 
                ON index_test (email)
                """;
            connector.create(createIndexSQL);
            log.info("B-Tree index creation test passed");
        } catch (SQLException e) {
            log.error("B-Tree index test failed: {}", e.getMessage());
            fail("B-Tree index creation should not throw exception");
        }
    }
    
    @Test
    @DisplayName("Test Create Unique Index")
    void testCreateUniqueIndex() {
        if (connector == null) {
            log.warn("Skipping unique index test - no database connection available");
            return;
        }
        
        try {
            String createTableSQL = """
                CREATE TABLE IF NOT EXISTS unique_index_test (
                    id SERIAL PRIMARY KEY,
                    username VARCHAR(50) UNIQUE,
                    email VARCHAR(100)
                )
                """;
            connector.create(createTableSQL);
            
            String createUniqueIndexSQL = """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_index_test_email 
                ON unique_index_test (email)
                """;
            connector.create(createUniqueIndexSQL);
            log.info("Unique index creation test passed");
        } catch (SQLException e) {
            log.error("Unique index test failed: {}", e.getMessage());
            fail("Unique index creation should not throw exception");
        }
    }
    
    @Test
    @DisplayName("Test Create Composite Index")
    void testCreateCompositeIndex() {
        if (connector == null) {
            log.warn("Skipping composite index test - no database connection available");
            return;
        }
        
        try {
            String createTableSQL = """
                CREATE TABLE IF NOT EXISTS composite_index_test (
                    id SERIAL PRIMARY KEY,
                    first_name VARCHAR(50),
                    last_name VARCHAR(50),
                    department VARCHAR(50),
                    salary NUMERIC(10,2)
                )
                """;
            connector.create(createTableSQL);
            
            String createCompositeIndexSQL = """
                CREATE INDEX IF NOT EXISTS idx_composite_index_test_name_dept 
                ON composite_index_test (first_name, last_name, department)
                """;
            connector.create(createCompositeIndexSQL);
            log.info("Composite index creation test passed");
        } catch (SQLException e) {
            log.error("Composite index test failed: {}", e.getMessage());
            fail("Composite index creation should not throw exception");
        }
    }
    
    @Test
    @DisplayName("Test Create Partial Index")
    void testCreatePartialIndex() {
        if (connector == null) {
            log.warn("Skipping partial index test - no database connection available");
            return;
        }
        
        try {
            String createTableSQL = """
                CREATE TABLE IF NOT EXISTS partial_index_test (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(100),
                    status VARCHAR(20),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """;
            connector.create(createTableSQL);
            
            String createPartialIndexSQL = """
                CREATE INDEX IF NOT EXISTS idx_partial_index_test_active 
                ON partial_index_test (name) 
                WHERE status = 'active'
                """;
            connector.create(createPartialIndexSQL);
            log.info("Partial index creation test passed");
        } catch (SQLException e) {
            log.error("Partial index test failed: {}", e.getMessage());
            fail("Partial index creation should not throw exception");
        }
    }
    
    @Test
    @DisplayName("Test List Indexes")
    void testListIndexes() {
        if (connector == null) {
            log.warn("Skipping list indexes test - no database connection available");
            return;
        }
        
        try {
            String createTableSQL = """
                CREATE TABLE IF NOT EXISTS list_indexes_test (
                    id SERIAL PRIMARY KEY,
                    data VARCHAR(100)
                )
                """;
            connector.create(createTableSQL);
            
            String createIndexSQL = """
                CREATE INDEX IF NOT EXISTS idx_list_indexes_test_data 
                ON list_indexes_test (data)
                """;
            connector.create(createIndexSQL);
            
            String listIndexesSQL = """
                SELECT indexname, tablename 
                FROM pg_indexes 
                WHERE tablename = 'list_indexes_test'
                """;
            ResultSet resultSet = connector.read(listIndexesSQL);
            
            boolean foundIndex = false;
            while (resultSet.next()) {
                String indexName = resultSet.getString("indexname");
                if (indexName.contains("idx_list_indexes_test_data")) {
                    foundIndex = true;
                    break;
                }
            }
            resultSet.close();
            
            assertTrue(foundIndex, "Created index should be found in pg_indexes");
            log.info("List indexes test passed");
        } catch (SQLException e) {
            log.error("List indexes test failed: {}", e.getMessage());
            fail("List indexes operation should not throw exception");
        }
    }
    
    @Test
    @DisplayName("Test Drop Index")
    void testDropIndex() {
        if (connector == null) {
            log.warn("Skipping drop index test - no database connection available");
            return;
        }
        
        try {
            String createTableSQL = """
                CREATE TABLE IF NOT EXISTS drop_index_test (
                    id SERIAL PRIMARY KEY,
                    value VARCHAR(100)
                )
                """;
            connector.create(createTableSQL);
            
            String createIndexSQL = """
                CREATE INDEX IF NOT EXISTS idx_drop_index_test_value 
                ON drop_index_test (value)
                """;
            connector.create(createIndexSQL);
            
            String dropIndexSQL = """
                DROP INDEX IF EXISTS idx_drop_index_test_value
                """;
            connector.delete(dropIndexSQL);
            log.info("Drop index test passed");
        } catch (SQLException e) {
            log.error("Drop index test failed: {}", e.getMessage());
            fail("Drop index operation should not throw exception");
        }
    }
}