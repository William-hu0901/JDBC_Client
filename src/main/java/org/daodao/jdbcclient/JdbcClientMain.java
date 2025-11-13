package org.daodao.jdbcclient;

import lombok.extern.slf4j.Slf4j;
import org.daodao.jdbcclient.connectors.PostgresConnector;
import org.daodao.jdbcclient.util.Constants;

import java.sql.ResultSet;
import java.sql.SQLException;

@Slf4j
public class JdbcClientMain {


    public static void main(String[] args) {
        new JdbcClientMain().run();
    }

    private void run() {
        actionOnPostgres();

    }
    private void actionOnPostgres() {
        PostgresConnector postgresConnector = null;
        try {
            // Example usage for PostgreSQL
            postgresConnector = new PostgresConnector(
                    Constants.POSTGRES_HOST,
                    Constants.POSTGRES_PORT,
                    Constants.POSTGRES_DB,
                    Constants.POSTGRES_USER,
                    Constants.POSTGRES_PASSWORD
            );

            postgresConnector.connect();
            log.info("Successfully connected to PostgreSQL database.");
            ResultSet resultSet = postgresConnector.read(Constants.POSTGRES_SQL);
            int count = 0;
            while (resultSet.next() && count < 2) {
                log.info("User ID: {}, Name: {}", resultSet.getInt("user_id"), resultSet.getString("username"));
                count++;
            }

        } catch (Exception e) {
            log.error("Error occurred: ", e);
        }finally {
            if(postgresConnector != null) postgresConnector.disconnect();
        }

    }
}