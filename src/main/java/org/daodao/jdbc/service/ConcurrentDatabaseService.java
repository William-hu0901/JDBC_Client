package org.daodao.jdbc.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.daodao.jdbc.mapper.MySqlUserMapper;
import org.daodao.jdbc.mapper.PostgresUserMapper;
import org.daodao.jdbc.model.User;
import org.daodao.jdbc.pool.DatabaseThreadPoolManager;
import org.daodao.jdbc.pool.MySqlConnectionPool;
import org.daodao.jdbc.pool.PostgresConnectionPool;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class ConcurrentDatabaseService {
    
    private final ExecutorService executorService;
    private final MySqlConnectionPool mysqlPool;
    private final PostgresConnectionPool postgresPool;
    private final SqlSessionFactory mysqlSqlSessionFactory;
    private final SqlSessionFactory postgresSqlSessionFactory;
    
    public ConcurrentDatabaseService(MySqlConnectionPool mysqlPool, PostgresConnectionPool postgresPool,
                                   SqlSessionFactory mysqlSqlSessionFactory, SqlSessionFactory postgresSqlSessionFactory) {
        this.executorService = DatabaseThreadPoolManager.getInstance().getExecutorService();
        this.mysqlPool = mysqlPool;
        this.postgresPool = postgresPool;
        this.mysqlSqlSessionFactory = mysqlSqlSessionFactory;
        this.postgresSqlSessionFactory = postgresSqlSessionFactory;
    }
    
    public CompletableFuture<Integer> concurrentMysqlInsert(List<User> users, int threadCount) {
        return CompletableFuture.supplyAsync(() -> {
            AtomicInteger successCount = new AtomicInteger(0);
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            
            // Ensure we don't create more threads than users
            final int actualThreadCount = Math.min(threadCount, users.size());
            final int batchSize = actualThreadCount > 0 ? users.size() / actualThreadCount : 0;
            
            log.debug("MySQL insert: {} users, {} threads, {} batch size", users.size(), actualThreadCount, batchSize);
            
            for (int i = 0; i < actualThreadCount; i++) {
                final int threadIndex = i;
                final int startIndex = i * batchSize;
                final int endIndex = (i == actualThreadCount - 1) ? users.size() : (i + 1) * batchSize;
                final List<User> userBatch = users.subList(startIndex, endIndex);
                
                CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                    try {
                        if (!userBatch.isEmpty()) {
                            int count = executeMysqlBatchInsert(userBatch);
                            successCount.addAndGet(count);
                            log.debug("Thread {} completed inserting {} users", threadIndex, count);
                        }
                    } catch (Exception e) {
                        log.error("Thread {} failed during MySQL insert", threadIndex, e);
                    }
                }, executorService);
                
                futures.add(future);
            }
            
            // Handle case where threadCount is 0 or users is empty
            if (actualThreadCount == 0 && !users.isEmpty()) {
                CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                    try {
                        int count = executeMysqlBatchInsert(users);
                        successCount.addAndGet(count);
                        log.debug("Single thread completed inserting {} users", count);
                    } catch (Exception e) {
                        log.error("Single thread failed during MySQL insert", e);
                    }
                }, executorService);
                
                futures.add(future);
            }
            
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
            return successCount.get();
        }, executorService);
    }
    
    public CompletableFuture<Integer> concurrentPostgresInsert(List<User> users, int threadCount) {
        return CompletableFuture.supplyAsync(() -> {
            AtomicInteger successCount = new AtomicInteger(0);
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            
            // Ensure we don't create more threads than users
            final int actualThreadCount = Math.min(threadCount, users.size());
            final int batchSize = actualThreadCount > 0 ? users.size() / actualThreadCount : 0;
            
            log.debug("PostgreSQL insert: {} users, {} threads, {} batch size", users.size(), actualThreadCount, batchSize);
            
            for (int i = 0; i < actualThreadCount; i++) {
                final int threadIndex = i;
                final int startIndex = i * batchSize;
                final int endIndex = (i == actualThreadCount - 1) ? users.size() : (i + 1) * batchSize;
                final List<User> userBatch = users.subList(startIndex, endIndex);
                
                CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                    try {
                        if (!userBatch.isEmpty()) {
                            int count = executePostgresBatchInsert(userBatch);
                            successCount.addAndGet(count);
                            log.debug("Thread {} completed inserting {} users", threadIndex, count);
                        }
                    } catch (Exception e) {
                        log.error("Thread {} failed during PostgreSQL insert", threadIndex, e);
                    }
                }, executorService);
                
                futures.add(future);
            }
            
            // Handle case where threadCount is 0 or users is empty
            if (actualThreadCount == 0 && !users.isEmpty()) {
                CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                    try {
                        int count = executePostgresBatchInsert(users);
                        successCount.addAndGet(count);
                        log.debug("Single thread completed inserting {} users", count);
                    } catch (Exception e) {
                        log.error("Single thread failed during PostgreSQL insert", e);
                    }
                }, executorService);
                
                futures.add(future);
            }
            
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
            return successCount.get();
        }, executorService);
    }
    
    public CompletableFuture<List<User>> concurrentMysqlQuery(List<User> users, int threadCount) {
        return CompletableFuture.supplyAsync(() -> {
            List<User> results = new ArrayList<>();
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            
            // Extract usernames for querying
            List<String> usernames = users.stream().map(User::getUsername).collect(java.util.stream.Collectors.toList());
            
            // Ensure we don't create more threads than usernames
            int actualThreadCount = Math.min(threadCount, usernames.size());
            int batchSize = actualThreadCount > 0 ? usernames.size() / actualThreadCount : 0;
            
            log.debug("MySQL query: {} usernames, {} threads, {} batch size", usernames.size(), actualThreadCount, batchSize);
            
            for (int i = 0; i < actualThreadCount; i++) {
                final int threadIndex = i;
                final int startIndex = i * batchSize;
                final int endIndex = (i == actualThreadCount - 1) ? usernames.size() : (i + 1) * batchSize;
                final List<String> usernameBatch = usernames.subList(startIndex, endIndex);
                
                CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                    try {
                        if (!usernameBatch.isEmpty()) {
                            List<User> usersResult = executeMysqlBatchQuery(usernameBatch);
                            synchronized (results) {
                                results.addAll(usersResult);
                            }
                            log.debug("Thread {} completed querying {} users", threadIndex, usersResult.size());
                        }
                    } catch (Exception e) {
                        log.error("Thread {} failed during MySQL query", threadIndex, e);
                    }
                }, executorService);
                
                futures.add(future);
            }
            
            // Handle case where threadCount is 0 or usernames is empty
            if (actualThreadCount == 0 && !usernames.isEmpty()) {
                CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                    try {
                        List<User> usersResult = executeMysqlBatchQuery(usernames);
                        synchronized (results) {
                            results.addAll(usersResult);
                        }
                        log.debug("Single thread completed querying {} users", usersResult.size());
                    } catch (Exception e) {
                        log.error("Single thread failed during MySQL query", e);
                    }
                }, executorService);
                
                futures.add(future);
            }
            
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
            return results;
        }, executorService);
    }
    
    public CompletableFuture<List<User>> concurrentPostgresQuery(List<String> usernames, int threadCount) {
        return CompletableFuture.supplyAsync(() -> {
            List<User> results = new ArrayList<>();
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            
            // Ensure we don't create more threads than usernames
            int actualThreadCount = Math.min(threadCount, usernames.size());
            int batchSize = actualThreadCount > 0 ? usernames.size() / actualThreadCount : 0;
            
            log.debug("PostgreSQL query: {} usernames, {} threads, {} batch size", usernames.size(), actualThreadCount, batchSize);
            
            for (int i = 0; i < actualThreadCount; i++) {
                final int threadIndex = i;
                final int startIndex = i * batchSize;
                final int endIndex = (i == actualThreadCount - 1) ? usernames.size() : (i + 1) * batchSize;
                final List<String> usernameBatch = usernames.subList(startIndex, endIndex);
                
                CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                    try {
                        if (!usernameBatch.isEmpty()) {
                            List<User> users = executePostgresBatchQuery(usernameBatch);
                            synchronized (results) {
                                results.addAll(users);
                            }
                            log.debug("Thread {} completed querying {} users", threadIndex, users.size());
                        }
                    } catch (Exception e) {
                        log.error("Thread {} failed during PostgreSQL query", threadIndex, e);
                    }
                }, executorService);
                
                futures.add(future);
            }
            
            // Handle case where threadCount is 0 or usernames is empty
            if (actualThreadCount == 0 && !usernames.isEmpty()) {
                CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                    try {
                        List<User> users = executePostgresBatchQuery(usernames);
                        synchronized (results) {
                            results.addAll(users);
                        }
                        log.debug("Single thread completed querying {} users", users.size());
                    } catch (Exception e) {
                        log.error("Single thread failed during PostgreSQL query", e);
                    }
                }, executorService);
                
                futures.add(future);
            }
            
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
            return results;
        }, executorService);
    }
    
    private int executeMysqlBatchInsert(List<User> users) throws Exception {
        return mysqlPool.executeQuery(connection -> {
            String sql = "INSERT IGNORE INTO users (username, email, age, city) VALUES (?, ?, ?, ?)";
            try (PreparedStatement stmt = connection.prepareStatement(sql)) {
                int count = 0;
                for (User user : users) {
                    stmt.setString(1, user.getUsername());
                    stmt.setString(2, user.getEmail());
                    stmt.setInt(3, user.getAge());
                    stmt.setString(4, user.getCity());
                    stmt.addBatch();
                }
                int[] results = stmt.executeBatch();
                for (int result : results) {
                    if (result > 0) count++;
                }
                return count;
            }
        });
    }
    
    private int executePostgresBatchInsert(List<User> users) throws Exception {
        return postgresPool.executeQuery(connection -> {
            String sql = "INSERT INTO users (username, email, age, city, created_at, updated_at) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) ON CONFLICT (username) DO NOTHING";
            try (PreparedStatement stmt = connection.prepareStatement(sql)) {
                int count = 0;
                for (User user : users) {
                    stmt.setString(1, user.getUsername());
                    stmt.setString(2, user.getEmail());
                    stmt.setInt(3, user.getAge());
                    stmt.setString(4, user.getCity());
                    stmt.addBatch();
                }
                int[] results = stmt.executeBatch();
                for (int result : results) {
                    if (result > 0) count++;
                }
                return count;
            }
        });
    }
    
    private List<User> executeMysqlBatchQuery(List<String> usernames) throws Exception {
        return mysqlPool.executeQuery(connection -> {
            List<User> users = new ArrayList<>();
            String sql = "SELECT id, username, email, age, city FROM users WHERE username IN (" + 
                         String.join(",", usernames.stream().map(u -> "?").toArray(String[]::new)) + ")";
            try (PreparedStatement stmt = connection.prepareStatement(sql)) {
                for (int i = 0; i < usernames.size(); i++) {
                    stmt.setString(i + 1, usernames.get(i));
                }
                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        User user = new User();
                        user.setId(rs.getInt("id"));
                        user.setUsername(rs.getString("username"));
                        user.setEmail(rs.getString("email"));
                        user.setAge(rs.getObject("age", Integer.class));
                        user.setCity(rs.getString("city"));
                        users.add(user);
                    }
                }
            }
            return users;
        });
    }
    
    private List<User> executePostgresBatchQuery(List<String> usernames) throws Exception {
        return postgresPool.executeQuery(connection -> {
            List<User> users = new ArrayList<>();
            String sql = "SELECT id, username, email, age, city, created_at as createdAt, updated_at as updatedAt FROM users WHERE username IN (" + 
                         String.join(",", usernames.stream().map(u -> "?").toArray(String[]::new)) + ")";
            try (PreparedStatement stmt = connection.prepareStatement(sql)) {
                for (int i = 0; i < usernames.size(); i++) {
                    stmt.setString(i + 1, usernames.get(i));
                }
                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        User user = new User();
                        user.setId(rs.getInt("id"));
                        user.setUsername(rs.getString("username"));
                        user.setEmail(rs.getString("email"));
                        user.setAge(rs.getObject("age", Integer.class));
                        user.setCity(rs.getString("city"));
                        
                        // Safely handle timestamp fields
                        try {
                            java.sql.Timestamp createdAt = rs.getTimestamp("createdAt");
                            if (createdAt != null) {
                                user.setCreatedAt(createdAt.toLocalDateTime());
                            }
                        } catch (Exception e) {
                            log.debug("Could not parse createdAt timestamp for user: {}", user.getUsername());
                        }
                        
                        try {
                            java.sql.Timestamp updatedAt = rs.getTimestamp("updatedAt");
                            if (updatedAt != null) {
                                user.setUpdatedAt(updatedAt.toLocalDateTime());
                            }
                        } catch (Exception e) {
                            log.debug("Could not parse updatedAt timestamp for user: {}", user.getUsername());
                        }
                        
                        users.add(user);
                    }
                }
            }
            return users;
        });
    }
    
    public CompletableFuture<Void> shutdown() {
        return CompletableFuture.runAsync(() -> {
            try {
                executorService.shutdown();
                if (!executorService.awaitTermination(30, TimeUnit.SECONDS)) {
                    executorService.shutdownNow();
                }
                log.info("ConcurrentDatabaseService shutdown completed");
            } catch (InterruptedException e) {
                log.error("Interrupted during shutdown", e);
                executorService.shutdownNow();
                Thread.currentThread().interrupt();
            }
        });
    }
}