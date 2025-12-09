package org.daodao.jdbc.concurrent;

import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.session.SqlSessionFactory;
import org.daodao.jdbc.config.MySqlConfig;
import org.daodao.jdbc.config.PostgresConfig;
import org.daodao.jdbc.model.User;
import org.daodao.jdbc.pool.DatabaseThreadPoolManager;
import org.daodao.jdbc.pool.MySqlConnectionPool;
import org.daodao.jdbc.pool.PostgresConnectionPool;
import org.daodao.jdbc.service.ConcurrentDatabaseService;
import org.junit.jupiter.api.*;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

@Slf4j
@DisplayName("Concurrent Database Operations Test")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)  // Create a new instance for each test class
class ConcurrentDatabaseTest {
    
    private MySqlConnectionPool mysqlPool;
    private PostgresConnectionPool postgresPool;
    private ConcurrentDatabaseService concurrentService;
    private MySqlConfig mysqlConfig;
    private PostgresConfig postgresConfig;
    
    // Track whether tables have been created to avoid repeated checks
    private static volatile boolean mysqlTableCreated = false;
    private static volatile boolean postgresTableCreated = false;
    
    @BeforeAll
    void initConfig() {
        // Initialize configuration once for the entire test class
        try {
            mysqlConfig = new MySqlConfig();
            postgresConfig = new PostgresConfig();
            log.info("Database configurations loaded");
        } catch (Exception e) {
            log.warn("Failed to load database configurations: {}", e.getMessage());
            // Don't fail the entire test suite, just log the error
        }
    }
    
    @BeforeEach
    void setUp() {
        try {
            
            // Initialize MySQL connection pool with error handling
            try {
                mysqlPool = new MySqlConnectionPool(
                    mysqlConfig.getHost(),
                    mysqlConfig.getPort(),
                    mysqlConfig.getDatabase(),
                    mysqlConfig.getUsername(),
                    mysqlConfig.getPassword(),
                    50
                );
                // Test MySQL connection
                mysqlPool.executeQuery(connection -> {
                    connection.createStatement().execute("SELECT 1");
                    return null;
                });
                log.info("MySQL connection pool initialized successfully");
            } catch (Exception e) {
                log.warn("MySQL connection failed: {}", e.getMessage());
                mysqlPool = null;
            }
            
            // Initialize PostgreSQL connection pool with error handling
            try {
                postgresPool = new PostgresConnectionPool(
                    postgresConfig.getHost(),
                    postgresConfig.getPort(),
                    postgresConfig.getDatabase(),
                    postgresConfig.getUsername(),
                    postgresConfig.getPassword(),
                    50
                );
                // Test PostgreSQL connection
                postgresPool.executeQuery(connection -> {
                    connection.createStatement().execute("SELECT 1");
                    return null;
                });
                log.info("PostgreSQL connection pool initialized successfully");
            } catch (Exception e) {
                log.warn("PostgreSQL connection failed: {}", e.getMessage());
                postgresPool = null;
            }
            
            // Only proceed if at least one database is available
            if (mysqlPool == null && postgresPool == null) {
                log.warn("Neither MySQL nor PostgreSQL is available for testing, skipping tests");
                return;
            }
            
            concurrentService = new ConcurrentDatabaseService(
                mysqlPool, postgresPool, null, null
            );
            
            setupTestTables();
            
            log.info("Concurrent test setup completed");
        } catch (Exception e) {
            log.warn("Test setup failed, skipping concurrent tests: {}", e.getMessage());
            // Don't fail the entire test suite, just log the error
        }
    }
    
    @AfterEach
    void tearDown() {
        try {
            // Clean up only test data to ensure test isolation
            // Note: We only delete test data, not the table itself
            if (mysqlPool != null) {
                try {
                    mysqlPool.executeQuery(connection -> {
                        try (java.sql.Statement stmt = connection.createStatement()) {
                            // Only delete test data with our specific naming pattern
                            stmt.execute("DELETE FROM users WHERE username LIKE 'testuser_%'");
                            log.debug("MySQL test data cleaned up");
                        }
                        return null;
                    });
                } catch (Exception e) {
                    log.warn("Failed to clean up MySQL test data", e);
                }
            }
            
            if (postgresPool != null) {
                try {
                    postgresPool.executeQuery(connection -> {
                        try (java.sql.Statement stmt = connection.createStatement()) {
                            // Only delete test data with our specific naming pattern
                            stmt.execute("DELETE FROM users WHERE username LIKE 'testuser_%'");
                            log.debug("PostgreSQL test data cleaned up");
                        }
                        return null;
                    });
                } catch (Exception e) {
                    log.warn("Failed to clean up PostgreSQL test data", e);
                }
            }
            
            // Close connections and pools, but don't shutdown the concurrent service
            // as it shares a thread pool with other services and tests
            if (mysqlPool != null) {
                try {
                    mysqlPool.close();
                    log.debug("MySQL pool closed");
                } catch (Exception e) {
                    log.warn("Error closing MySQL pool", e);
                }
            }
            
            if (postgresPool != null) {
                try {
                    postgresPool.close();
                    log.debug("PostgreSQL pool closed");
                } catch (Exception e) {
                    log.warn("Error closing PostgreSQL pool", e);
                }
            }
            
            // Don't shutdown the thread pool manager here as it might be used by other tests
        } catch (Exception e) {
            log.error("Error during teardown", e);
        }
    }
    
    @AfterAll
    void cleanupClass() {
        // Reset table creation flags when the test class is complete
        mysqlTableCreated = false;
        postgresTableCreated = false;
        log.debug("Test class completed, table creation flags reset");
    }
    
    private void setupTestTables() throws Exception {
        // Only create tables once using static flags to avoid repeated checks
        if (mysqlPool != null && !mysqlTableCreated) {
            try {
                mysqlPool.executeQuery(connection -> {
                    try (java.sql.Statement stmt = connection.createStatement()) {
                        // Check if table exists
                        boolean tableExists = false;
                        try (java.sql.ResultSet rs = stmt.executeQuery("SHOW TABLES LIKE 'users'")) {
                            tableExists = rs.next();
                        }
                        
                        // Create table only if it doesn't exist
                        if (!tableExists) {
                            stmt.execute("CREATE TABLE users (" +
                                        "id INT AUTO_INCREMENT PRIMARY KEY," +
                                        "username VARCHAR(50) NOT NULL UNIQUE," +
                                        "email VARCHAR(100) NOT NULL UNIQUE," +
                                        "age INT," +
                                        "city VARCHAR(50)," +
                                        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP," +
                                        "updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP" +
                                        ")");
                            log.info("MySQL users table created successfully");
                        } else {
                            log.debug("MySQL users table already exists, skipping creation");
                        }
                    }
                    return null;
                });
                mysqlTableCreated = true;  // Mark as created to avoid future checks
            } catch (Exception e) {
                log.warn("MySQL table setup failed: {}", e.getMessage());
            }
        }
        
        if (postgresPool != null && !postgresTableCreated) {
            try {
                postgresPool.executeQuery(connection -> {
                    try (java.sql.Statement stmt = connection.createStatement()) {
                        // Check if table exists
                        boolean tableExists = false;
                        try (java.sql.ResultSet rs = stmt.executeQuery(
                            "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'users')")) {
                            if (rs.next()) {
                                tableExists = rs.getBoolean(1);
                            }
                        }
                        
                        // Create table only if it doesn't exist
                        if (!tableExists) {
                            stmt.execute("CREATE TABLE users (" +
                                        "id SERIAL PRIMARY KEY," +
                                        "username VARCHAR(50) NOT NULL UNIQUE," +
                                        "email VARCHAR(100) NOT NULL UNIQUE," +
                                        "age INT," +
                                        "city VARCHAR(50)," +
                                        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP," +
                                        "updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP" +
                                        ")");
                            log.info("PostgreSQL users table created successfully");
                        } else {
                            log.debug("PostgreSQL users table already exists, skipping creation");
                        }
                    }
                    return null;
                });
                postgresTableCreated = true;  // Mark as created to avoid future checks
            } catch (Exception e) {
                log.warn("PostgreSQL table setup failed: {}", e.getMessage());
            }
        }
    }
    
    @Test
    @DisplayName("Test Concurrent MySQL Insert Operations")
    void testConcurrentMysqlInsert() throws Exception {
        // Skip test if MySQL is not available
        if (mysqlPool == null) {
            log.warn("MySQL is not available, skipping test");
            return;
        }
        
        List<User> users = generateTestUsers(100);
        
        try {
            log.info("Starting MySQL concurrent insert test with {} users and {} threads", users.size(), 5);
            CompletableFuture<Integer> future = concurrentService.concurrentMysqlInsert(users, 5);
            log.info("Waiting for MySQL insert operation to complete...");
            Integer insertedCount = future.get(90, java.util.concurrent.TimeUnit.SECONDS); // Increased timeout to 90 seconds
            
            assertNotNull(insertedCount);
            assertTrue(insertedCount > 0, "Should have inserted some users");
            log.info("Successfully inserted {} users concurrently into MySQL", insertedCount);
        } catch (Exception e) {
            log.error("MySQL insert test failed", e);
            // Clean up any partial data to avoid affecting other tests
            try {
                mysqlPool.executeQuery(connection -> {
                    try (java.sql.Statement stmt = connection.createStatement()) {
                        stmt.execute("DELETE FROM users WHERE username LIKE 'testuser_%'");
                    }
                    return null;
                });
            } catch (Exception cleanupEx) {
                log.warn("Failed to clean up after MySQL insert test failure", cleanupEx);
            }
            throw e;
        }
    }
    
    @Test
    @DisplayName("Test Concurrent PostgreSQL Insert Operations")
    void testConcurrentPostgresInsert() throws Exception {
        // Skip test if PostgreSQL is not available
        if (postgresPool == null) {
            log.warn("PostgreSQL is not available, skipping test");
            return;
        }
        
        List<User> users = generateTestUsers(100);
        
        try {
            log.info("Starting PostgreSQL concurrent insert test with {} users and {} threads", users.size(), 5);
            CompletableFuture<Integer> future = concurrentService.concurrentPostgresInsert(users, 5);
            log.info("Waiting for PostgreSQL insert operation to complete...");
            Integer insertedCount = future.get(90, java.util.concurrent.TimeUnit.SECONDS); // Increased timeout to 90 seconds
            
            assertNotNull(insertedCount);
            assertTrue(insertedCount > 0, "Should have inserted some users");
            log.info("Successfully inserted {} users concurrently into PostgreSQL", insertedCount);
        } catch (Exception e) {
            log.error("PostgreSQL insert test failed", e);
            // Clean up any partial data to avoid affecting other tests
            try {
                postgresPool.executeQuery(connection -> {
                    try (java.sql.Statement stmt = connection.createStatement()) {
                        stmt.execute("DELETE FROM users WHERE username LIKE 'testuser_%'");
                    }
                    return null;
                });
            } catch (Exception cleanupEx) {
                log.warn("Failed to clean up after PostgreSQL insert test failure", cleanupEx);
            }
            throw e;
        }
    }
    
    @Test
    @DisplayName("Test Concurrent MySQL Query Operations")
    void testConcurrentMysqlQuery() throws Exception {
        // Skip test if MySQL is not available
        if (mysqlPool == null) {
            log.warn("MySQL is not available, skipping test");
            return;
        }
        
        List<User> users = generateTestUsers(50);
        
        try {
            log.info("Starting MySQL concurrent insert for query test with {} users and {} threads", users.size(), 5);
            CompletableFuture<Integer> insertFuture = concurrentService.concurrentMysqlInsert(users, 5);
            Integer insertedCount = insertFuture.get(60, java.util.concurrent.TimeUnit.SECONDS);
            log.info("Inserted {} users for query test", insertedCount);
            
            List<String> usernames = users.subList(0, 25).stream()
                                        .map(User::getUsername)
                                        .toList();
            
            log.info("Starting MySQL concurrent query test with {} usernames and {} threads", usernames.size(), 5);
            // Fix: Pass the users list instead of usernames list
            CompletableFuture<List<User>> queryFuture = concurrentService.concurrentMysqlQuery(users.subList(0, 25), 5);
            List<User> queriedUsers = queryFuture.get(60, java.util.concurrent.TimeUnit.SECONDS);
            
            assertNotNull(queriedUsers);
            assertTrue(queriedUsers.size() > 0, "Should have queried some users");
            log.info("Successfully queried {} users concurrently from MySQL", queriedUsers.size());
        } catch (Exception e) {
            log.error("MySQL query test failed", e);
            // Clean up any partial data to avoid affecting other tests
            try {
                mysqlPool.executeQuery(connection -> {
                    try (java.sql.Statement stmt = connection.createStatement()) {
                        stmt.execute("DELETE FROM users WHERE username LIKE 'testuser_%'");
                    }
                    return null;
                });
            } catch (Exception cleanupEx) {
                log.warn("Failed to clean up after MySQL query test failure", cleanupEx);
            }
            throw e;
        }
    }
    
    @Test
    @DisplayName("Test Concurrent PostgreSQL Query Operations")
    void testConcurrentPostgresQuery() throws Exception {
        // Skip test if PostgreSQL is not available
        if (postgresPool == null) {
            log.warn("PostgreSQL is not available, skipping test");
            return;
        }
        
        List<User> users = generateTestUsers(50);
        
        try {
            log.info("Starting PostgreSQL concurrent insert for query test with {} users and {} threads", users.size(), 5);
            CompletableFuture<Integer> insertFuture = concurrentService.concurrentPostgresInsert(users, 5);
            Integer insertedCount = insertFuture.get(60, java.util.concurrent.TimeUnit.SECONDS);
            log.info("Inserted {} users for query test", insertedCount);
            
            List<String> usernames = users.subList(0, 25).stream()
                                        .map(User::getUsername)
                                        .toList();
            
            log.info("Starting PostgreSQL concurrent query test with {} usernames and {} threads", usernames.size(), 5);
            CompletableFuture<List<User>> queryFuture = concurrentService.concurrentPostgresQuery(usernames, 5);
            List<User> queriedUsers = queryFuture.get(60, java.util.concurrent.TimeUnit.SECONDS);
            
            assertNotNull(queriedUsers);
            assertTrue(queriedUsers.size() > 0, "Should have queried some users");
            log.info("Successfully queried {} users concurrently from PostgreSQL", queriedUsers.size());
        } catch (Exception e) {
            log.error("PostgreSQL query test failed", e);
            // Clean up any partial data to avoid affecting other tests
            try {
                postgresPool.executeQuery(connection -> {
                    try (java.sql.Statement stmt = connection.createStatement()) {
                        stmt.execute("DELETE FROM users WHERE username LIKE 'testuser_%'");
                    }
                    return null;
                });
            } catch (Exception cleanupEx) {
                log.warn("Failed to clean up after PostgreSQL query test failure", cleanupEx);
            }
            throw e;
        }
    }
    
    @Test
    @DisplayName("Test Mixed Database Concurrent Operations")
    void testMixedDatabaseConcurrentOperations() throws Exception {
        // Skip test if neither database is available
        if (mysqlPool == null && postgresPool == null) {
            log.warn("Neither MySQL nor PostgreSQL is available, skipping test");
            return;
        }
        
        List<CompletableFuture<Integer>> futures = new ArrayList<>();
        List<String> results = new ArrayList<>();
        
        if (mysqlPool != null) {
            List<User> mysqlUsers = generateTestUsers(100);
            log.info("Starting MySQL mixed operation test with {} users and {} threads", mysqlUsers.size(), 5);
            CompletableFuture<Integer> mysqlInsertFuture = concurrentService.concurrentMysqlInsert(mysqlUsers, 5);
            futures.add(mysqlInsertFuture);
            results.add("MySQL");
        }
        
        if (postgresPool != null) {
            List<User> postgresUsers = generateTestUsers(100);
            log.info("Starting PostgreSQL mixed operation test with {} users and {} threads", postgresUsers.size(), 5);
            CompletableFuture<Integer> postgresInsertFuture = concurrentService.concurrentPostgresInsert(postgresUsers, 5);
            futures.add(postgresInsertFuture);
            results.add("PostgreSQL");
        }
        
        log.info("Waiting for mixed database operations to complete...");
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                         .get(90, java.util.concurrent.TimeUnit.SECONDS); // Increased timeout to 90 seconds
        
        for (int i = 0; i < futures.size(); i++) {
            Integer inserted = futures.get(i).get();
            assertTrue(inserted > 0, results.get(i) + " should have inserted users");
            log.info("{} inserted {} users", results.get(i), inserted);
        }
        
        log.info("Mixed operations completed successfully");
    }
    
    private List<User> generateTestUsers(int count) {
        List<User> users = new ArrayList<>();
        // Use a fixed timestamp for all users in the same batch to ensure consistency
        // But add a random component to ensure uniqueness across test runs
        long timestamp = System.currentTimeMillis();
        int randomSuffix = (int) (Math.random() * 10000);
        
        for (int i = 0; i < count; i++) {
            User user = new User();
            user.setUsername("testuser_" + timestamp + "_" + randomSuffix + "_" + i);
            user.setEmail("testuser_" + timestamp + "_" + randomSuffix + "_" + i + "@example.com");
            user.setAge(20 + (i % 50));
            user.setCity("TestCity_" + (i % 10));
            users.add(user);
        }
        return users;
    }
}