package org.daodao.jdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.bson.Document;
import org.daodao.jdbc.config.PostgresConfig;
import org.daodao.jdbc.connectors.MongoConnector;
import org.daodao.jdbc.connectors.PostgresConnector;
import org.daodao.jdbc.exceptions.MongoException;
import org.daodao.jdbc.exceptions.PropertyException;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class JdbcClientMain {
    
    private static final Logger log = LoggerFactory.getLogger(JdbcClientMain.class);

    public static void main(String[] args) {
        new JdbcClientMain().run();
    }

    private void run() {
        try {
            actionOnMongoDB();
            actionOnPostgres();
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
                    log.info("User ID: {}, Name: {}", resultSet.getInt("user_id"), resultSet.getString("username"));
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
}