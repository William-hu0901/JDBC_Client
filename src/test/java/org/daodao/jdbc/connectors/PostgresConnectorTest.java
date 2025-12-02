package org.daodao.jdbc.connectors;

import org.daodao.jdbc.config.PostgresConfig;
import org.junit.jupiter.api.*;

import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PostgresConnectorTest {

    private PostgresConnector postgresConnector;
    private PostgresConfig config;
    private static final String TEST_TABLE_NAME = "test_users";

    @BeforeAll
    void setUp() throws SQLException {
        config = new PostgresConfig();
        postgresConnector = new PostgresConnector(
                config.getPostgresHost(),
                config.getPostgresPort(),
                config.getPostgresDatabase(),
                config.getPostgresUsername(),
                config.getPostgresPassword()
        );
        postgresConnector.connect();
        
        // Create test table
        String createTableSQL = String.format("""
            CREATE TABLE IF NOT EXISTS %s (
                user_id SERIAL PRIMARY KEY,
                username VARCHAR(50) NOT NULL,
                email VARCHAR(100) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )""", TEST_TABLE_NAME);
        postgresConnector.create(createTableSQL);
    }

    @AfterAll
    void tearDown() {
        if (postgresConnector != null) {
            // Drop test table
            try {
                String dropTableSQL = String.format("DROP TABLE IF EXISTS %s", TEST_TABLE_NAME);
                postgresConnector.delete(dropTableSQL);
            } catch (SQLException e) {
                // Ignore drop table errors during cleanup
            }
            postgresConnector.disconnect();
        }
    }

    @BeforeEach
    void clearTestData() throws SQLException {
        // Clear test data before each test
        String deleteSQL = String.format("DELETE FROM %s", TEST_TABLE_NAME);
        postgresConnector.delete(deleteSQL);
    }

    @Test
    void testCreate() throws SQLException {
        // Test CREATE operation
        String insertSQL = String.format("""
            INSERT INTO %s (username, email) 
            VALUES ('testuser', 'testuser@example.com')""", TEST_TABLE_NAME);
        
        postgresConnector.create(insertSQL);
        
        // Verify the record was created
        String selectSQL = String.format("SELECT COUNT(*) as count FROM %s WHERE username = 'testuser'", TEST_TABLE_NAME);
        ResultSet resultSet = postgresConnector.read(selectSQL);
        
        assertTrue(resultSet.next());
        assertEquals(1, resultSet.getInt("count"));
    }

    @Test
    void testRead() throws SQLException {
        // Insert test data
        String insertSQL = String.format("""
            INSERT INTO %s (username, email) 
            VALUES ('readuser', 'readuser@example.com')""", TEST_TABLE_NAME);
        postgresConnector.create(insertSQL);
        
        // Test READ operation
        String selectSQL = String.format("SELECT * FROM %s WHERE username = 'readuser'", TEST_TABLE_NAME);
        ResultSet resultSet = postgresConnector.read(selectSQL);
        
        assertTrue(resultSet.next());
        assertEquals("readuser", resultSet.getString("username"));
        assertEquals("readuser@example.com", resultSet.getString("email"));
        assertFalse(resultSet.next()); // Should only have one record
    }

    @Test
    void testUpdate() throws SQLException {
        // Insert test data
        String insertSQL = String.format("""
            INSERT INTO %s (username, email) 
            VALUES ('updateuser', 'updateuser@example.com')""", TEST_TABLE_NAME);
        postgresConnector.create(insertSQL);
        
        // Test UPDATE operation
        String updateSQL = String.format("""
            UPDATE %s 
            SET email = 'updated@example.com' 
            WHERE username = 'updateuser'""", TEST_TABLE_NAME);
        postgresConnector.update(updateSQL);
        
        // Verify the update
        String selectSQL = String.format("SELECT email FROM %s WHERE username = 'updateuser'", TEST_TABLE_NAME);
        ResultSet resultSet = postgresConnector.read(selectSQL);
        
        assertTrue(resultSet.next());
        assertEquals("updated@example.com", resultSet.getString("email"));
    }

    @Test
    void testDelete() throws SQLException {
        // Insert test data
        String insertSQL = String.format("""
            INSERT INTO %s (username, email) 
            VALUES ('deleteuser', 'deleteuser@example.com')""", TEST_TABLE_NAME);
        postgresConnector.create(insertSQL);
        
        // Verify record exists
        String countSQL = String.format("SELECT COUNT(*) as count FROM %s WHERE username = 'deleteuser'", TEST_TABLE_NAME);
        ResultSet countResult = postgresConnector.read(countSQL);
        countResult.next();
        assertEquals(1, countResult.getInt("count"));
        
        // Test DELETE operation
        String deleteSQL = String.format("DELETE FROM %s WHERE username = 'deleteuser'", TEST_TABLE_NAME);
        postgresConnector.delete(deleteSQL);
        
        // Verify the deletion
        ResultSet resultSet = postgresConnector.read(countSQL);
        assertTrue(resultSet.next());
        assertEquals(0, resultSet.getInt("count"));
    }

    @Test
    void testReadWithEmptyResult() throws SQLException {
        // Test READ operation with no results
        String selectSQL = String.format("SELECT * FROM %s WHERE username = 'nonexistent'", TEST_TABLE_NAME);
        ResultSet resultSet = postgresConnector.read(selectSQL);
        
        assertFalse(resultSet.next()); // Should have no results
    }

    @Test
    void testCreateWithInvalidSQL() {
        // Test CREATE operation with invalid SQL
        String invalidSQL = "INVALID SQL STATEMENT";
        
        assertThrows(SQLException.class, () -> {
            postgresConnector.create(invalidSQL);
        });
    }

    @Test
    void testConnectionIsValid() throws SQLException {
        // Test that connection is valid
        assertNotNull(postgresConnector);
        
        // Simple query to test connection
        String testSQL = String.format("SELECT 1 as test_value FROM %s LIMIT 1", TEST_TABLE_NAME);
        ResultSet resultSet = postgresConnector.read(testSQL);
        
        // Should not throw exception
        assertNotNull(resultSet);
    }
}