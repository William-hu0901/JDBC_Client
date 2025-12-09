package org.daodao.jdbc;

import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.daodao.jdbc.config.MySqlConfig;
import org.daodao.jdbc.config.PostgresConfig;
import org.daodao.jdbc.connectors.MongoConnector;
import org.daodao.jdbc.connectors.MySqlConnector;
import org.daodao.jdbc.connectors.PostgresConnector;
import org.daodao.jdbc.exceptions.MongoException;
import org.daodao.jdbc.exceptions.MySqlException;
import org.daodao.jdbc.exceptions.PropertyException;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

@Slf4j
public class JdbcClientMain {

    public static void main(String[] args) {
        new JdbcClientMain().run();
    }

    private void run() {
        try {
            actionOnMySQL();
            actionOnPostgres();
            actionOnMongoDB();
        } catch (Exception e) {
            log.error("Application error: ", e);
        }
    }
    
    private void actionOnMongoDB() {
        MongoConnector mongoConnector = null;
        try {
            mongoConnector = new MongoConnector();
            mongoConnector.connect();
            log.info("Successfully connected to MongoDB database.");
            
            if (mongoConnector.isDatabaseEmpty()) {
                log.info("Database is empty, initializing with sample data...");
                mongoConnector.initializeDatabase();
                log.info("Database initialized successfully.");
            }
            
            List<Document> allDocuments = mongoConnector.findAllDocuments();
            log.info("Total documents in database: {}", allDocuments.size());
            
            for (Document doc : allDocuments) {
                log.info("Document: Name={}, Email={}, Age={}, City={}",
                        doc.getString("name"), 
                        doc.getString("email"), 
                        doc.getInteger("age"),
                        doc.getString("city"));
            }
            
            Document johnDoc = mongoConnector.findDocumentByEmail("john.doe@example.com");
            if (johnDoc != null) {
                log.info("Found John Doe: {}", johnDoc.toJson());
                
                boolean updated = mongoConnector.updateDocumentByEmail("john.doe@example.com", "Boston", 31);
                if (updated) {
                    log.info("Successfully updated John Doe's information");
                }
            }
            
            List<Document> youngUsers = mongoConnector.findDocumentsByAgeRange(20, 30);
            log.info("Found {} users aged between 20-30", youngUsers.size());
            
        } catch (MongoException e) {
            log.error("MongoDB error occurred: ", e);
        } catch (Exception e) {
            log.error("Unexpected error occurred with MongoDB: ", e);
        } finally {
            if (mongoConnector != null) mongoConnector.disconnect();
        }
    }
    
    private void actionOnPostgres() {
        PostgresConnector postgresConnector = null;
        try {
            // Load configuration from application.properties
            PostgresConfig config = new PostgresConfig();
            
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
            try (ResultSet resultSet = postgresConnector.read(config.getPostgresSql())) {
                int count = 0;
                while (resultSet.next() && count < 2) {
                    log.info("User ID: {}, Name: {}", resultSet.getInt("id"), resultSet.getString("username"));
                    count++;
                }
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
    
    private void actionOnMySQL() {
        MySqlConnector mysqlConnector = null;
        try {
            // Load configuration from application.properties
            MySqlConfig config = new MySqlConfig();
            
            // Create MySQL connector using configuration
            mysqlConnector = new MySqlConnector(config);
            
            mysqlConnector.connect();
            log.info("Successfully connected to MySQL database.");
            
            // Initialize database if empty
            if (mysqlConnector.isDatabaseEmpty()) {
                log.info("Database is empty, initializing with sample data...");
                mysqlConnector.initializeDatabase();
                log.info("Database initialized successfully.");
            }
            
            // Find all users
            List<String> allUsers = mysqlConnector.findAllUsers();
            log.info("Total users in database: {}", allUsers.size());
            
            // Display first few users
            int displayCount = Math.min(3, allUsers.size());
            for (int i = 0; i < displayCount; i++) {
                log.info("User {}: {}", i + 1, allUsers.get(i));
            }
            
            // Find users by city
            List<String> newYorkUsers = mysqlConnector.findUsersByCity("New York");
            log.info("Found {} users in New York", newYorkUsers.size());
            
            // Get total user count
            int userCount = mysqlConnector.getUserCount();
            log.info("Total user count: {}", userCount);
            
            // Test CRUD operations
            String testUsername = "test_user_" + System.currentTimeMillis();
            String testEmail = testUsername + "@example.com";
            
            // Create
            boolean inserted = mysqlConnector.insertUser(testUsername, testEmail, 25, "Test City");
            if (inserted) {
                log.info("Successfully inserted test user: {}", testUsername);
            }
            
            // Update
            String newEmail = "updated_" + testEmail;
            boolean updated = mysqlConnector.updateUserEmail(testUsername, newEmail);
            if (updated) {
                log.info("Successfully updated email for user: {}", testUsername);
            }
            
            // Delete
            boolean deleted = mysqlConnector.deleteUser(testUsername);
            if (deleted) {
                log.info("Successfully deleted test user: {}", testUsername);
            }
            
        } catch (MySqlException e) {
            log.error("MySQL error occurred: ", e);
        } catch (Exception e) {
            log.error("Unexpected error occurred with MySQL: ", e);
        } finally {
            if (mysqlConnector != null) mysqlConnector.disconnect();
        }
    }
}