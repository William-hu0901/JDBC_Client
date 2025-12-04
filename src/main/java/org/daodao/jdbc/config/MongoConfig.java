package org.daodao.jdbc.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class MongoConfig {
    
    private final String connectionString;
    private final String databaseName;
    private final String collectionName;
    
    public MongoConfig() {
        Properties props = new Properties();
        try (InputStream input = MongoConfig.class.getClassLoader().getResourceAsStream("application.properties")) {
            if (input == null) {
                throw new RuntimeException("Unable to find application.properties");
            }
            props.load(input);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load application.properties", e);
        }
        
        this.connectionString = props.getProperty("mongodb.connection.string");
        this.databaseName = props.getProperty("mongodb.database.name");
        this.collectionName = props.getProperty("mongodb.collection.name");
    }
    
    public MongoConfig(String connectionString, String databaseName, String collectionName) {
        this.connectionString = connectionString;
        this.databaseName = databaseName;
        this.collectionName = collectionName;
    }
    
    public String getConnectionString() {
        return connectionString;
    }
    
    public String getDatabaseName() {
        return databaseName;
    }
    
    public String getCollectionName() {
        return collectionName;
    }
}