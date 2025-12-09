package org.daodao.jdbc.pool;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

@Slf4j
public class MySqlConnectionPool {
    
    private final ConnectionPool<Connection> connectionPool;
    private static final int DEFAULT_POOL_SIZE = 20;
    
    public MySqlConnectionPool(String host, int port, String database, String username, String password) {
        this(host, port, database, username, password, DEFAULT_POOL_SIZE);
    }
    
    public MySqlConnectionPool(String host, int port, String database, String username, String password, int poolSize) {
        String url = String.format("jdbc:mysql://%s:%d/%s?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC", 
                                   host, port, database);
        
        ConnectionPool.ConnectionFactory<Connection> factory = new ConnectionPool.ConnectionFactory<>() {
            @Override
            public Connection create() throws SQLException {
                Properties props = new Properties();
                props.setProperty("user", username);
                props.setProperty("password", password);
                props.setProperty("useSSL", "false");
                props.setProperty("allowPublicKeyRetrieval", "true");
                props.setProperty("serverTimezone", "UTC");
                return DriverManager.getConnection(url, props);
            }
            
            @Override
            public boolean isValid(Connection connection) {
                try {
                    if (connection == null || connection.isClosed()) {
                        return false;
                    }
                    if (connection.isValid(5)) {
                        return true;
                    }
                } catch (SQLException e) {
                    log.debug("Connection validation failed", e);
                }
                return false;
            }
            
            @Override
            public void close(Connection connection) {
                try {
                    if (connection != null && !connection.isClosed()) {
                        connection.close();
                    }
                } catch (SQLException e) {
                    log.error("Error closing MySQL connection", e);
                }
            }
        };
        
        this.connectionPool = new ConnectionPool<>(poolSize, factory);
        log.info("MySQL connection pool created for database: {}@{}:{}/{}", username, host, port, database);
    }
    
    public <R> R executeQuery(ConnectionQueryOperation<R> operation) throws Exception {
        return connectionPool.executeWithConnection(connection -> {
            try {
                return operation.execute(connection);
            } catch (SQLException e) {
                log.error("Error executing MySQL query", e);
                throw new RuntimeException("MySQL query failed", e);
            }
        });
    }
    
    @FunctionalInterface
    public interface ConnectionQueryOperation<R> {
        R execute(Connection connection) throws SQLException;
    }
    
    public void close() {
        connectionPool.close();
    }
    
    public int getActiveConnections() {
        return connectionPool.getActiveConnections();
    }
    
    public int getTotalConnections() {
        return connectionPool.getTotalConnections();
    }
    
    public int getAvailableConnections() {
        return connectionPool.getAvailableConnections();
    }
}