package org.daodao.jdbcclient;

import lombok.extern.slf4j.Slf4j;
import org.daodao.jdbcclient.config.DatabaseConfig;
import org.daodao.jdbcclient.connectors.PostgresConnector;
import org.daodao.jdbcclient.exceptions.PropertyException;

import java.sql.ResultSet;
import java.sql.SQLException;

@Slf4j
public class JdbcClientMain {

    public static void main(String[] args) {
        new JdbcClientMain().run();
    }

    private void run() {
        try {
            actionOnPostgres();
        } catch (Exception e) {
            log.error("Application error: ", e);
        }
    }
    
    private void actionOnPostgres() {
        PostgresConnector postgresConnector = null;
        try {
            // Load configuration from application.properties
            DatabaseConfig config = new DatabaseConfig();
            
            // Create PostgreSQL connector using configuration
            postgresConnector = new PostgresConnector(
                    config.getPostgresHost(),
                    config.getPostgresPort(),
                    config.getPostgresDatabase(),
                    config.getPostgresUsername(),
                    config.getPostgresPassword()
            );

            postgresConnector.connect();
            log.info("Successfully connected to PostgreSQL database.");
            
            // Execute query from configuration
            ResultSet resultSet = postgresConnector.read(config.getPostgresSql());
            int count = 0;
            while (resultSet.next() && count < 2) {
                log.info("User ID: {}, Name: {}", resultSet.getInt("user_id"), resultSet.getString("username"));
                count++;
            }

        } catch (SQLException e) {
            log.error("Database error occurred: ", e);
        } catch (PropertyException e) {
            log.error("Configuration error occurred: ", e);
        } catch (Exception e) {
            log.error("Unexpected error occurred: ", e);
        } finally {
            if (postgresConnector != null) postgresConnector.disconnect();
        }
    }
}