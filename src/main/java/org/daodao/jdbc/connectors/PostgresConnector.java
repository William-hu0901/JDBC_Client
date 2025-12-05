package org.daodao.jdbc.connectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.*;
import java.util.Properties;
public class PostgresConnector {
    
    private static final Logger log = LoggerFactory.getLogger(PostgresConnector.class);
    private final String hostname;
    private final int port;
    private final String database;
    private final String username;
    private final String password;
    private Connection connection;

    public PostgresConnector(String hostname, int port, String database, String username, String password) {
        this.hostname = hostname;
        this.port = port;
        this.database = database;
        this.username = username;
        this.password = password;
    }

    public void connect() throws SQLException {
        Properties props = new Properties();
        props.setProperty("user", username);
        props.setProperty("password", password);
        //enable ssl, but not mandatory to validate the certificate
        String url = String.format("jdbc:postgresql://%s:%d/%s?sslmode=prefer", hostname, port, database);
        log.info("Connecting to PostgreSQL database at: {}", url);
        connection = DriverManager.getConnection(url, props);
    }

    public void disconnect() {
        try {
            if (connection != null && !connection.isClosed()) {
                connection.close();
                log.info("Disconnected from PostgreSQL database.");
            }
        } catch (SQLException e) {
            log.error("Error while disconnecting from PostgreSQL database: ", e);
        }
    }

    public void create(String query) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate(query);
            log.info("Executed CREATE query: {}", query);
        }
    }

    public ResultSet read(String query) throws SQLException {
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(query);
        log.info("Executed READ query: {}", query);
        // Note: ResultSet and Statement must be closed by the caller
        return resultSet;
    }
    
    /**
     * Safe read method that executes query and returns single value
     * Automatically handles resource cleanup
     */
    public int readSingleValue(String query) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(query)) {
            if (resultSet.next()) {
                return resultSet.getInt(1);
            }
            return 0;
        }
    }
    
    /**
     * Safe read method that executes query and returns boolean
     * Automatically handles resource cleanup
     */
    public boolean readBoolean(String query) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(query)) {
            return resultSet.next();
        }
    }

    public void update(String query) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate(query);
            log.info("Executed UPDATE query: {}", query);
        }
    }

    public void delete(String query) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate(query);
            log.info("Executed DELETE query: {}", query);
        }
    }
}