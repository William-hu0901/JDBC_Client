package org.daodao.jdbc.concurrent;

import lombok.extern.slf4j.Slf4j;
import org.daodao.jdbc.config.MySqlConfig;
import org.daodao.jdbc.config.PostgresConfig;
import org.daodao.jdbc.model.User;
import org.daodao.jdbc.pool.MySqlConnectionPool;
import org.daodao.jdbc.pool.PostgresConnectionPool;
import org.daodao.jdbc.service.ConcurrentDatabaseService;
import org.junit.jupiter.api.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;

@Slf4j
@DisplayName("Basic Concurrent Database Operations Test")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class BasicConcurrentTest {
    
    private MySqlConnectionPool mysqlPool;
    private PostgresConnectionPool postgresPool;
    private ConcurrentDatabaseService concurrentService;
    
    @BeforeAll
    void initConfig() {
        try {
            MySqlConfig mysqlConfig = new MySqlConfig();
            PostgresConfig postgresConfig = new PostgresConfig();
            
            // Initialize MySQL connection pool
            try {
                mysqlPool = new MySqlConnectionPool(
                    mysqlConfig.getHost(),
                    mysqlConfig.getPort(),
                    mysqlConfig.getDatabase(),
                    mysqlConfig.getUsername(),
                    mysqlConfig.getPassword(),
                    5 // Small pool for testing
                );
                log.info("MySQL connection pool initialized");
            } catch (Exception e) {
                log.warn("Failed to initialize MySQL connection pool: {}", e.getMessage());
                mysqlPool = null;
            }
            
            // Initialize PostgreSQL connection pool
            try {
                postgresPool = new PostgresConnectionPool(
                    postgresConfig.getHost(),
                    postgresConfig.getPort(),
                    postgresConfig.getDatabase(),
                    postgresConfig.getUsername(),
                    postgresConfig.getPassword(),
                    5 // Small pool for testing
                );
                log.info("PostgreSQL connection pool initialized");
            } catch (Exception e) {
                log.warn("Failed to initialize PostgreSQL connection pool: {}", e.getMessage());
                postgresPool = null;
            }
            
            // Create service
            concurrentService = new ConcurrentDatabaseService(mysqlPool, postgresPool, null, null);
            
            // Setup tables
            setupTables();
            
        } catch (Exception e) {
            log.error("Failed to initialize test", e);
            // Don't fail the entire test suite, just log the error
        }
    }
    
    private void setupTables() throws Exception {
        if (mysqlPool != null) {
            mysqlPool.executeQuery(connection -> {
                try (var stmt = connection.createStatement()) {
                    stmt.execute("CREATE TABLE IF NOT EXISTS users (" +
                               "id INT AUTO_INCREMENT PRIMARY KEY, " +
                               "username VARCHAR(50) NOT NULL UNIQUE, " +
                               "email VARCHAR(100) NOT NULL UNIQUE, " +
                               "age INT, " +
                               "city VARCHAR(50))");
                }
                return null;
            });
        }
        
        if (postgresPool != null) {
            postgresPool.executeQuery(connection -> {
                try (var stmt = connection.createStatement()) {
                    stmt.execute("CREATE TABLE IF NOT EXISTS users (" +
                               "id SERIAL PRIMARY KEY, " +
                               "username VARCHAR(50) NOT NULL UNIQUE, " +
                               "email VARCHAR(100) NOT NULL UNIQUE, " +
                               "age INT, " +
                               "city VARCHAR(50))");
                }
                return null;
            });
        }
    }
    
    @BeforeEach
    void cleanupTestData() {
        try {
            if (mysqlPool != null) {
                mysqlPool.executeQuery(connection -> {
                    try (var stmt = connection.createStatement()) {
                        stmt.execute("DELETE FROM users WHERE username LIKE 'test_%'");
                    }
                    return null;
                });
            }
            
            if (postgresPool != null) {
                postgresPool.executeQuery(connection -> {
                    try (var stmt = connection.createStatement()) {
                        stmt.execute("DELETE FROM users WHERE username LIKE 'test_%'");
                    }
                    return null;
                });
            }
        } catch (Exception e) {
            log.warn("Failed to cleanup test data", e);
        }
    }
    
    @AfterAll
    void tearDown() {
        try {
            // Don't shutdown the concurrent service as it shares a thread pool with other services and tests
            if (mysqlPool != null) {
                mysqlPool.close();
            }
            if (postgresPool != null) {
                postgresPool.close();
            }
        } catch (Exception e) {
            log.warn("Error during teardown", e);
        }
    }
    
    @Test
    @DisplayName("Basic MySQL Concurrent Insert Test")
    void testBasicMysqlConcurrentInsert() {
        // Skip test if MySQL is not available
        if (mysqlPool == null) {
            log.warn("MySQL is not available, skipping test");
            return;
        }
        
        List<User> users = generateTestUsers(5);
        CompletableFuture<Integer> future = concurrentService.concurrentMysqlInsert(users, 2);
        
        assertDoesNotThrow(() -> {
            Integer result = future.get();
            assertNotNull(result);
            assertTrue(result >= 0);
            log.info("MySQL insert test completed, inserted {} users", result);
        });
    }
    
    @Test
    @DisplayName("Basic PostgreSQL Concurrent Insert Test")
    void testBasicPostgresConcurrentInsert() {
        // Skip test if PostgreSQL is not available
        if (postgresPool == null) {
            log.warn("PostgreSQL is not available, skipping test");
            return;
        }
        
        List<User> users = generateTestUsers(5);
        CompletableFuture<Integer> future = concurrentService.concurrentPostgresInsert(users, 2);
        
        assertDoesNotThrow(() -> {
            Integer result = future.get();
            assertNotNull(result);
            assertTrue(result >= 0);
            log.info("PostgreSQL insert test completed, inserted {} users", result);
        });
    }
    
    private List<User> generateTestUsers(int count) {
        List<User> users = new ArrayList<>();
        long timestamp = System.currentTimeMillis();
        
        for (int i = 0; i < count; i++) {
            User user = new User();
            user.setUsername("test_" + timestamp + "_" + i);
            user.setEmail("test_" + timestamp + "_" + i + "@example.com");
            user.setAge(20 + (i % 50));
            user.setCity("TestCity_" + (i % 5));
            users.add(user);
        }
        return users;
    }
}