package org.daodao.jdbc.pool;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class ConnectionPool<T> implements AutoCloseable {
    
    private final BlockingQueue<T> pool;
    private final AtomicInteger activeConnections = new AtomicInteger(0);
    private final AtomicInteger totalConnections = new AtomicInteger(0);
    private final int maxSize;
    private final ConnectionFactory<T> connectionFactory;
    private volatile boolean isShutdown = false;
    
    public interface ConnectionFactory<T> {
        T create() throws Exception;
        boolean isValid(T connection);
        void close(T connection);
    }
    
    public ConnectionPool(int maxSize, ConnectionFactory<T> connectionFactory) {
        this.maxSize = maxSize;
        this.connectionFactory = connectionFactory;
        this.pool = new LinkedBlockingQueue<>(maxSize);
        log.info("Connection pool initialized with max size: {}", maxSize);
    }
    
    public T getConnection() throws Exception {
        if (isShutdown) {
            throw new IllegalStateException("Connection pool is shutdown");
        }
        
        T connection = pool.poll();
        if (connection != null) {
            if (connectionFactory.isValid(connection)) {
                activeConnections.incrementAndGet();
                log.debug("Retrieved connection from pool, active: {}", activeConnections.get());
                return connection;
            } else {
                // Connection is invalid, close it and decrement total connections
                try {
                    connectionFactory.close(connection);
                } catch (Exception e) {
                    log.warn("Error closing invalid connection", e);
                }
                totalConnections.decrementAndGet();
                log.debug("Removed invalid connection from pool, total: {}", totalConnections.get());
            }
        }
        
        // If no valid connection available, create a new one if we haven't reached max size
        if (totalConnections.get() < maxSize) {
            T newConnection = connectionFactory.create();
            totalConnections.incrementAndGet();
            activeConnections.incrementAndGet();
            log.debug("Created new connection, total: {}, active: {}", totalConnections.get(), activeConnections.get());
            return newConnection;
        }
        
        // If we've reached max size, wait for a connection to become available
        try {
            // Wait for up to 30 seconds for a connection
            T pooledConnection = pool.poll(30, TimeUnit.SECONDS);
            if (pooledConnection != null) {
                if (connectionFactory.isValid(pooledConnection)) {
                    activeConnections.incrementAndGet();
                    log.debug("Retrieved connection from pool after waiting, active: {}", activeConnections.get());
                    return pooledConnection;
                } else {
                    // Connection is invalid, close it and decrement total connections
                    try {
                        connectionFactory.close(pooledConnection);
                    } catch (Exception e) {
                        log.warn("Error closing invalid connection", e);
                    }
                    totalConnections.decrementAndGet();
                    log.debug("Removed invalid connection from pool after waiting, total: {}", totalConnections.get());
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new Exception("Interrupted while waiting for connection", e);
        }
        
        // If we still don't have a connection, try one more time with a shorter timeout
        T finalConnection = pool.poll(5, TimeUnit.SECONDS);
        if (finalConnection != null && connectionFactory.isValid(finalConnection)) {
            activeConnections.incrementAndGet();
            log.debug("Retrieved connection from pool on final attempt, active: {}", activeConnections.get());
            return finalConnection;
        } else if (finalConnection != null) {
            // Connection is invalid, close it and decrement total connections
            try {
                connectionFactory.close(finalConnection);
            } catch (Exception e) {
                log.warn("Error closing invalid connection", e);
            }
            totalConnections.decrementAndGet();
            log.debug("Removed invalid connection from pool on final attempt, total: {}", totalConnections.get());
        }
        
        throw new Exception("Unable to acquire valid connection from pool after multiple attempts");
    }
    
    public void releaseConnection(T connection) {
        if (connection == null) {
            return;
        }
        
        try {
            if (connectionFactory.isValid(connection)) {
                // Return valid connection to pool
                if (pool.offer(connection)) {
                    activeConnections.decrementAndGet();
                    log.debug("Returned connection to pool, active: {}", activeConnections.get());
                } else {
                    // Pool is full, close the connection
                    connectionFactory.close(connection);
                    totalConnections.decrementAndGet();
                    activeConnections.decrementAndGet();
                    log.debug("Pool full, closed connection, total: {}, active: {}", totalConnections.get(), activeConnections.get());
                }
            } else {
                // Connection is invalid, close it
                connectionFactory.close(connection);
                totalConnections.decrementAndGet();
                activeConnections.decrementAndGet();
                log.debug("Closed invalid connection, total: {}, active: {}", totalConnections.get(), activeConnections.get());
            }
        } catch (Exception e) {
            log.error("Error releasing connection", e);
            // Even if there's an error, we should decrement the active count
            activeConnections.decrementAndGet();
        }
    }
    
    public <R> R executeWithConnection(ConnectionOperation<T, R> operation) throws Exception {
        T connection = null;
        try {
            connection = getConnection();
            return operation.execute(connection);
        } finally {
            if (connection != null) {
                releaseConnection(connection);
            }
        }
    }
    
    @FunctionalInterface
    public interface ConnectionOperation<T, R> {
        R execute(T connection) throws Exception;
    }
    
    @Override
    public void close() {
        isShutdown = true;
        log.info("Shutting down connection pool...");
        
        T connection;
        while ((connection = pool.poll()) != null) {
            try {
                connectionFactory.close(connection);
                totalConnections.decrementAndGet();
            } catch (Exception e) {
                log.error("Error closing connection during shutdown", e);
            }
        }
        
        log.info("Connection pool shutdown completed");
    }
    
    public int getActiveConnections() {
        return activeConnections.get();
    }
    
    public int getTotalConnections() {
        return totalConnections.get();
    }
    
    public int getAvailableConnections() {
        return pool.size();
    }
    
    public boolean isShutdown() {
        return isShutdown;
    }
}