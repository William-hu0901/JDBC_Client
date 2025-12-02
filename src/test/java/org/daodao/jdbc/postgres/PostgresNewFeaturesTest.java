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

/**
 * Test class for PostgreSQL latest production features.
 * 
 * This test covers the following new features introduced in recent PostgreSQL versions:
 * - JSONB data type and operators (PostgreSQL 9.4+)
 * - Generated columns (PostgreSQL 12+)
 * - Partitioning features (PostgreSQL 10+)
 * - UPSERT (INSERT ... ON CONFLICT) (PostgreSQL 9.5+)
 * - Window functions improvements
 * - Full-text search capabilities
 * - Array operations and functions
 */
public class PostgresNewFeaturesTest {
    
    private static final Logger log = LoggerFactory.getLogger(PostgresNewFeaturesTest.class);
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
            log.info("PostgreSQL connection established for new features tests");
        } catch (Exception e) {
            log.warn("Failed to connect to PostgreSQL for new features tests: {}", e.getMessage());
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
    @DisplayName("Test JSONB Data Type and Operations")
    void testJSONBOperations() {
        if (connector == null) {
            log.warn("Skipping JSONB test - no database connection available");
            return;
        }
        
        try {
            String createTableSQL = """
                CREATE TABLE IF NOT EXISTS jsonb_test (
                    id SERIAL PRIMARY KEY,
                    data JSONB,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """;
            connector.create(createTableSQL);
            
            String insertSQL = """
                INSERT INTO jsonb_test (data) 
                VALUES ('{"name": "John", "age": 30, "tags": ["developer", "java"]}')
                """;
            connector.update(insertSQL);
            
            String querySQL = """
                SELECT data->>'name' as name, data->>'age' as age 
                FROM jsonb_test 
                WHERE data ? 'name'
                """;
            ResultSet resultSet = connector.read(querySQL);
            
            assertTrue(resultSet.next(), "Should find inserted JSONB record");
            assertEquals("John", resultSet.getString("name"));
            assertEquals("30", resultSet.getString("age"));
            resultSet.close();
            
            log.info("JSONB operations test passed");
        } catch (SQLException e) {
            log.error("JSONB operations test failed: {}", e.getMessage());
            fail("JSONB operations should not throw exception");
        }
    }
    
    @Test
    @DisplayName("Test Generated Columns")
    void testGeneratedColumns() {
        if (connector == null) {
            log.warn("Skipping generated columns test - no database connection available");
            return;
        }
        
        try {
            String createTableSQL = """
                CREATE TABLE IF NOT EXISTS generated_columns_test (
                    id SERIAL PRIMARY KEY,
                    first_name VARCHAR(50),
                    last_name VARCHAR(50),
                    full_name VARCHAR(101) GENERATED ALWAYS AS (first_name || ' ' || last_name) STORED
                )
                """;
            connector.create(createTableSQL);
            
            String insertSQL = """
                INSERT INTO generated_columns_test (first_name, last_name) 
                VALUES ('Alice', 'Johnson')
                """;
            connector.update(insertSQL);
            
            String querySQL = """
                SELECT full_name FROM generated_columns_test 
                WHERE first_name = 'Alice' AND last_name = 'Johnson'
                """;
            ResultSet resultSet = connector.read(querySQL);
            
            assertTrue(resultSet.next(), "Should find inserted record");
            assertEquals("Alice Johnson", resultSet.getString("full_name"));
            resultSet.close();
            
            log.info("Generated columns test passed");
        } catch (SQLException e) {
            log.warn("Generated columns test failed (may not be supported in this PostgreSQL version): {}", e.getMessage());
        }
    }
    
    @Test
    @DisplayName("Test UPSERT (INSERT ... ON CONFLICT)")
    void testUpsertOperation() {
        if (connector == null) {
            log.warn("Skipping UPSERT test - no database connection available");
            return;
        }
        
        try {
            String createTableSQL = """
                CREATE TABLE IF NOT EXISTS upsert_test (
                    id SERIAL PRIMARY KEY,
                    email VARCHAR(100) UNIQUE,
                    name VARCHAR(100),
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """;
            connector.create(createTableSQL);
            
            String insertSQL = """
                INSERT INTO upsert_test (email, name) 
                VALUES ('test@example.com', 'Original Name')
                """;
            connector.update(insertSQL);
            
            String upsertSQL = """
                INSERT INTO upsert_test (email, name) 
                VALUES ('test@example.com', 'Updated Name')
                ON CONFLICT (email) 
                DO UPDATE SET name = EXCLUDED.name, updated_at = CURRENT_TIMESTAMP
                """;
            connector.update(upsertSQL);
            
            String querySQL = """
                SELECT name FROM upsert_test WHERE email = 'test@example.com'
                """;
            ResultSet resultSet = connector.read(querySQL);
            
            assertTrue(resultSet.next(), "Should find upserted record");
            assertEquals("Updated Name", resultSet.getString("name"));
            resultSet.close();
            
            log.info("UPSERT operation test passed");
        } catch (SQLException e) {
            log.error("UPSERT operation test failed: {}", e.getMessage());
            fail("UPSERT operation should not throw exception");
        }
    }
    
    @Test
    @DisplayName("Test Array Operations")
    void testArrayOperations() {
        if (connector == null) {
            log.warn("Skipping array operations test - no database connection available");
            return;
        }
        
        try {
            String createTableSQL = """
                CREATE TABLE IF NOT EXISTS array_test (
                    id SERIAL PRIMARY KEY,
                    tags TEXT[],
                    scores INTEGER[]
                )
                """;
            connector.create(createTableSQL);
            
            String insertSQL = """
                INSERT INTO array_test (tags, scores) 
                VALUES (ARRAY['java', 'postgresql', 'database'], ARRAY[95, 87, 92])
                """;
            connector.update(insertSQL);
            
            String querySQL = """
                SELECT tags[1] as first_tag, cardinality(scores) as score_count
                FROM array_test
                WHERE 'java' = ANY(tags)
                """;
            ResultSet resultSet = connector.read(querySQL);
            
            assertTrue(resultSet.next(), "Should find record with array data");
            assertEquals("java", resultSet.getString("first_tag"));
            assertEquals(3, resultSet.getInt("score_count"));
            resultSet.close();
            
            log.info("Array operations test passed");
        } catch (SQLException e) {
            log.error("Array operations test failed: {}", e.getMessage());
            fail("Array operations should not throw exception");
        }
    }
    
    @Test
    @DisplayName("Test Full-Text Search")
    void testFullTextSearch() {
        if (connector == null) {
            log.warn("Skipping full-text search test - no database connection available");
            return;
        }
        
        try {
            String createTableSQL = """
                CREATE TABLE IF NOT EXISTS fts_test (
                    id SERIAL PRIMARY KEY,
                    title VARCHAR(200),
                    content TEXT,
                    search_vector tsvector
                )
                """;
            connector.create(createTableSQL);
            
            String insertSQL = """
                INSERT INTO fts_test (title, content, search_vector) 
                VALUES (
                    'PostgreSQL Guide', 
                    'This is a comprehensive guide to PostgreSQL database features',
                    to_tsvector('english', 'This is a comprehensive guide to PostgreSQL database features')
                )
                """;
            connector.update(insertSQL);
            
            String searchSQL = """
                SELECT title FROM fts_test 
                WHERE search_vector @@ to_tsquery('english', 'PostgreSQL & guide')
                """;
            ResultSet resultSet = connector.read(searchSQL);
            
            assertTrue(resultSet.next(), "Should find record matching search query");
            assertEquals("PostgreSQL Guide", resultSet.getString("title"));
            resultSet.close();
            
            log.info("Full-text search test passed");
        } catch (SQLException e) {
            log.error("Full-text search test failed: {}", e.getMessage());
            fail("Full-text search should not throw exception");
        }
    }
    
    @Test
    @DisplayName("Test Window Functions")
    void testWindowFunctions() {
        if (connector == null) {
            log.warn("Skipping window functions test - no database connection available");
            return;
        }
        
        try {
            String createTableSQL = """
                CREATE TABLE IF NOT EXISTS window_test (
                    id SERIAL PRIMARY KEY,
                    department VARCHAR(50),
                    employee_name VARCHAR(100),
                    salary NUMERIC(10,2)
                )
                """;
            connector.create(createTableSQL);
            
            String insertSQL = """
                INSERT INTO window_test (department, employee_name, salary) 
                VALUES 
                    ('IT', 'Alice', 80000),
                    ('IT', 'Bob', 75000),
                    ('HR', 'Charlie', 65000),
                    ('HR', 'Diana', 70000),
                    ('IT', 'Eve', 85000)
                """;
            connector.update(insertSQL);
            
            String windowSQL = """
                SELECT employee_name, salary, 
                       ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) as dept_rank
                FROM window_test
                ORDER BY department, dept_rank
                """;
            ResultSet resultSet = connector.read(windowSQL);
            
            boolean foundRecord = false;
            while (resultSet.next()) {
                String name = resultSet.getString("employee_name");
                int rank = resultSet.getInt("dept_rank");
                if ("Eve".equals(name) && rank == 1) {
                    foundRecord = true;
                    break;
                }
            }
            resultSet.close();
            
            assertTrue(foundRecord, "Should find Eve ranked #1 in IT department");
            log.info("Window functions test passed");
        } catch (SQLException e) {
            log.error("Window functions test failed: {}", e.getMessage());
            fail("Window functions should not throw exception");
        }
    }
    
    @Test
    @DisplayName("Test Table Partitioning")
    void testTablePartitioning() {
        if (connector == null) {
            log.warn("Skipping table partitioning test - no database connection available");
            return;
        }
        
        try {
            String createPartitionedTableSQL = """
                CREATE TABLE IF NOT EXISTS partition_test (
                    id SERIAL,
                    created_at DATE,
                    data VARCHAR(100)
                ) PARTITION BY RANGE (created_at)
                """;
            connector.create(createPartitionedTableSQL);
            
            String createPartitionSQL = """
                CREATE TABLE IF NOT EXISTS partition_test_2024 
                PARTITION OF partition_test 
                FOR VALUES FROM ('2024-01-01') TO ('2025-01-01')
                """;
            connector.create(createPartitionSQL);
            
            String insertSQL = """
                INSERT INTO partition_test (created_at, data) 
                VALUES ('2024-06-15', 'Partitioned data')
                """;
            connector.update(insertSQL);
            
            String querySQL = """
                SELECT COUNT(*) as count FROM partition_test 
                WHERE created_at >= '2024-01-01' AND created_at < '2025-01-01'
                """;
            ResultSet resultSet = connector.read(querySQL);
            
            assertTrue(resultSet.next(), "Should find partitioned data");
            int count = resultSet.getInt("count");
            assertTrue(count >= 1, "Should find at least one record in partition");
            resultSet.close();
            
            log.info("Table partitioning test passed");
        } catch (SQLException e) {
            log.warn("Table partitioning test failed (may not be supported in this PostgreSQL version): {}", e.getMessage());
        }
    }
}