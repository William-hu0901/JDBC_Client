package org.daodao.jdbcclient.connectors;

import lombok.extern.slf4j.Slf4j;
import java.sql.*;
import java.util.Properties;

@Slf4j
public class PostgresConnector {
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
        return resultSet;
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