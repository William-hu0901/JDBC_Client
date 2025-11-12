package org.daodao.jdbcclient;

import lombok.extern.slf4j.Slf4j;
import org.daodao.jdbcclient.connectors.PostgresConnector;
import org.daodao.jdbcclient.util.Constants;

import java.sql.ResultSet;

@Slf4j
public class JdbcClientMain {

    public static void main(String[] args) {
        try {
            // Example usage for PostgreSQL
            PostgresConnector postgresConnector = new PostgresConnector(
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
        }
    }
}