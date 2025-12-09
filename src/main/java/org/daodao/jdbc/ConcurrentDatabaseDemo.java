package org.daodao.jdbc;

import lombok.extern.slf4j.Slf4j;
import org.daodao.jdbc.config.MySqlConfig;
import org.daodao.jdbc.config.PostgresConfig;
import org.daodao.jdbc.model.User;
import org.daodao.jdbc.pool.DatabaseThreadPoolManager;
import org.daodao.jdbc.pool.MySqlConnectionPool;
import org.daodao.jdbc.pool.PostgresConnectionPool;
import org.daodao.jdbc.service.ConcurrentDatabaseService;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class ConcurrentDatabaseDemo {
    
    public static void main(String[] args) {
        log.info("Starting Concurrent Database Demo...");
        
        try {
            MySqlConnectionPool mysqlPool = null;
            PostgresConnectionPool postgresPool = null;
            ConcurrentDatabaseService service = null;
            
            try {
                MySqlConfig mysqlConfig = new MySqlConfig();
                PostgresConfig postgresConfig = new PostgresConfig();
                
                // Initialize MySQL connection pool with error handling
                try {
                    mysqlPool = new MySqlConnectionPool(
                        mysqlConfig.getHost(),
                        mysqlConfig.getPort(),
                        mysqlConfig.getDatabase(),
                        mysqlConfig.getUsername(),
                        mysqlConfig.getPassword(),
                        30
                    );
                    log.info("MySQL connection pool initialized");
                    
                    // Check if table exists and has correct schema, recreate if needed
                    setupMysqlTableIfNeeded(mysqlPool);
                } catch (Exception e) {
                    log.warn("Failed to initialize MySQL connection pool: {}", e.getMessage());
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
                        30
                    );
                    log.info("PostgreSQL connection pool initialized");
                    
                    // Check if table exists and has correct schema, recreate if needed
                    setupPostgresTableIfNeeded(postgresPool);
                } catch (Exception e) {
                    log.warn("Failed to initialize PostgreSQL connection pool: {}", e.getMessage());
                    postgresPool = null;
                }
                
                // Check if at least one database is available
                if (mysqlPool == null && postgresPool == null) {
                    throw new RuntimeException("Neither MySQL nor PostgreSQL is available");
                }
                
                service = new ConcurrentDatabaseService(mysqlPool, postgresPool, null, null);
                
                List<User> testUsers = generateTestUsers(100);
                
                // Start MySQL insert test if pool is available
                CompletableFuture<Integer> mysqlFuture = null;
                if (mysqlPool != null) {
                    log.info("Starting concurrent MySQL insert test...");
                    mysqlFuture = service.concurrentMysqlInsert(testUsers, 2);
                } else {
                    log.warn("Skipping MySQL insert test - connection pool not available");
                }
                
                // Start PostgreSQL insert test if pool is available
                CompletableFuture<Integer> postgresFuture = null;
                if (postgresPool != null) {
                    log.info("Starting concurrent PostgreSQL insert test...");
                    postgresFuture = service.concurrentPostgresInsert(testUsers, 2);
                } else {
                    log.warn("Skipping PostgreSQL insert test - connection pool not available");
                }
                
                // Wait for operations to complete with timeout
                try {
                    // Create list of futures that are actually initialized
                    List<CompletableFuture<Integer>> futures = new ArrayList<>();
                    if (mysqlFuture != null) futures.add(mysqlFuture);
                    if (postgresFuture != null) futures.add(postgresFuture);
                    
                    if (futures.isEmpty()) {
                        throw new RuntimeException("No database operations were started");
                    }
                    
                    // Wait for all operations to complete
                    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(60, java.util.concurrent.TimeUnit.SECONDS);
                    
                    // Get results from available futures
                    Integer mysqlResults = mysqlFuture != null ? 
                        mysqlFuture.get(5, java.util.concurrent.TimeUnit.SECONDS) : 0;
                    Integer postgresResults = postgresFuture != null ? 
                        postgresFuture.get(5, java.util.concurrent.TimeUnit.SECONDS) : 0;
                    
                    log.info("Concurrent operations completed - MySQL: {} users, PostgreSQL: {} users", 
                            mysqlResults, postgresResults);
                } catch (java.util.concurrent.TimeoutException e) {
                    log.error("Timeout waiting for database operations to complete", e);
                    
                    // Try to get partial results if available
                    try {
                        Integer mysqlResults = (mysqlFuture != null && mysqlFuture.isDone()) ? mysqlFuture.get() : 0;
                        Integer postgresResults = (postgresFuture != null && postgresFuture.isDone()) ? postgresFuture.get() : 0;
                        
                        log.warn("Partial results - MySQL: {} users, PostgreSQL: {} users", 
                                mysqlResults, postgresResults);
                    } catch (Exception innerEx) {
                        log.error("Failed to get partial results", innerEx);
                    }
                }
                
                DatabaseThreadPoolManager manager = DatabaseThreadPoolManager.getInstance();
                log.info("Thread pool stats - Active: {}, Pool Size: {}, Completed: {}", 
                        manager.getActiveCount(), manager.getPoolSize(), manager.getCompletedTaskCount());
                
                if (mysqlPool != null) {
                    log.info("MySQL connection pool stats - Active: {}, Total: {}, Available: {}", 
                            mysqlPool.getActiveConnections(), mysqlPool.getTotalConnections(), mysqlPool.getAvailableConnections());
                }
                
                if (postgresPool != null) {
                    log.info("PostgreSQL connection pool stats - Active: {}, Total: {}, Available: {}", 
                            postgresPool.getActiveConnections(), postgresPool.getTotalConnections(), postgresPool.getAvailableConnections());
                }
                
            } finally {
                // Close resources in reverse order of creation
                try {
                    if (service != null) {
                        try {
                            service.shutdown().get(10, java.util.concurrent.TimeUnit.SECONDS);
                            log.info("Service shutdown completed");
                        } catch (Exception e) {
                            log.warn("Error during service shutdown", e);
                        }
                    }
                } finally {
                    try {
                        if (mysqlPool != null) {
                            try {
                                mysqlPool.close();
                                log.info("MySQL pool closed");
                            } catch (Exception e) {
                                log.warn("Error closing MySQL pool", e);
                            }
                        }
                    } finally {
                        try {
                            if (postgresPool != null) {
                                try {
                                    postgresPool.close();
                                    log.info("PostgreSQL pool closed");
                                } catch (Exception e) {
                                    log.warn("Error closing PostgreSQL pool", e);
                                }
                            }
                        } finally {
                            try {
                                // Shutdown thread pool manager last
                                DatabaseThreadPoolManager.getInstance().shutdown();
                                log.info("Thread pool manager shutdown completed");
                            } catch (Exception e) {
                                log.warn("Error shutting down thread pool manager", e);
                            }
                        }
                    }
                }
            }
            
        } catch (Exception e) {
            log.error("Demo failed", e);
        }
        
        log.info("Concurrent Database Demo completed");
    }
    
    private static List<User> generateTestUsers(int count) {
        List<User> users = new ArrayList<>();
        long timestamp = System.currentTimeMillis();
        
        for (int i = 0; i < count; i++) {
            User user = new User();
            user.setUsername("demo_user_" + timestamp + "_" + i);
            user.setEmail("demo_user_" + timestamp + "_" + i + "@example.com");
            user.setAge(20 + (i % 60));
            user.setCity("DemoCity_" + (i % 15));
            users.add(user);
        }
        
        return users;
    }
    
    /**
     * Check if MySQL users table exists and has correct schema, recreate if needed
     */
    private static void setupMysqlTableIfNeeded(MySqlConnectionPool pool) {
        try {
            pool.executeQuery(connection -> {
                try (java.sql.Statement stmt = connection.createStatement()) {
                    try {
                        // Check if table exists
                        boolean tableExists = false;
                        try (java.sql.ResultSet rs = stmt.executeQuery("SHOW TABLES LIKE 'users'")) {
                            tableExists = rs.next();
                        }
                        
                        if (tableExists) {
                            try {
                                // Check if table has all required columns
                                boolean hasRequiredColumns = false;
                                try (java.sql.ResultSet rs = stmt.executeQuery("DESCRIBE users")) {
                                    boolean hasId = false;
                                    boolean hasUsername = false;
                                    boolean hasEmail = false;
                                    boolean hasAge = false;
                                    boolean hasCity = false;
                                    
                                    while (rs.next()) {
                                        String columnName = rs.getString("Field");
                                        if ("id".equals(columnName)) hasId = true;
                                        else if ("username".equals(columnName)) hasUsername = true;
                                        else if ("email".equals(columnName)) hasEmail = true;
                                        else if ("age".equals(columnName)) hasAge = true;
                                        else if ("city".equals(columnName)) hasCity = true;
                                    }
                                    
                                    hasRequiredColumns = hasId && hasUsername && hasEmail && hasAge && hasCity;
                                }
                                
                                if (!hasRequiredColumns) {
                                    log.warn("MySQL users table exists but missing required columns, recreating table");
                                    stmt.execute("DROP TABLE users");
                                    tableExists = false;
                                }
                            } catch (Exception e) {
                                log.warn("Error checking MySQL table schema, recreating table: {}", e.getMessage());
                                try {
                                    stmt.execute("DROP TABLE users");
                                    tableExists = false;
                                } catch (Exception dropEx) {
                                    log.warn("Failed to drop MySQL table: {}", dropEx.getMessage());
                                }
                            }
                        }
                        
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
                            log.info("MySQL users table already exists with correct schema");
                        }
                    } catch (Exception e) {
                        log.error("Error setting up MySQL table", e);
                        throw e;
                    }
                }
                return null;
            });
        } catch (Exception e) {
            log.error("Failed to setup MySQL table", e);
        }
    }
    
    /**
     * Check if PostgreSQL users table exists and has correct schema, recreate if needed
     */
    private static void setupPostgresTableIfNeeded(PostgresConnectionPool pool) {
        try {
            pool.executeQuery(connection -> {
                try (java.sql.Statement stmt = connection.createStatement()) {
                    try {
                        // Check if table exists
                        boolean tableExists = false;
                        try (java.sql.ResultSet rs = stmt.executeQuery(
                            "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'users')")) {
                            if (rs.next()) {
                                tableExists = rs.getBoolean(1);
                            }
                        }
                        
                        if (tableExists) {
                            try {
                                // Check if table has all required columns
                                boolean hasRequiredColumns = false;
                                try (java.sql.ResultSet rs = stmt.executeQuery(
                                    "SELECT column_name FROM information_schema.columns WHERE table_name = 'users'")) {
                                    boolean hasId = false;
                                    boolean hasUsername = false;
                                    boolean hasEmail = false;
                                    boolean hasAge = false;
                                    boolean hasCity = false;
                                    
                                    while (rs.next()) {
                                        String columnName = rs.getString("column_name");
                                        if ("id".equals(columnName)) hasId = true;
                                        else if ("username".equals(columnName)) hasUsername = true;
                                        else if ("email".equals(columnName)) hasEmail = true;
                                        else if ("age".equals(columnName)) hasAge = true;
                                        else if ("city".equals(columnName)) hasCity = true;
                                    }
                                    
                                    hasRequiredColumns = hasId && hasUsername && hasEmail && hasAge && hasCity;
                                }
                                
                                if (!hasRequiredColumns) {
                                    log.warn("PostgreSQL users table exists but missing required columns, recreating table");
                                    stmt.execute("DROP TABLE users");
                                    tableExists = false;
                                }
                            } catch (Exception e) {
                                log.warn("Error checking PostgreSQL table schema, recreating table: {}", e.getMessage());
                                try {
                                    stmt.execute("DROP TABLE users");
                                    tableExists = false;
                                } catch (Exception dropEx) {
                                    log.warn("Failed to drop PostgreSQL table: {}", dropEx.getMessage());
                                }
                            }
                        }
                        
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
                            log.info("PostgreSQL users table already exists with correct schema");
                        }
                    } catch (Exception e) {
                        log.error("Error setting up PostgreSQL table", e);
                        throw e;
                    }
                }
                return null;
            });
        } catch (Exception e) {
            log.error("Failed to setup PostgreSQL table", e);
        }
    }
}