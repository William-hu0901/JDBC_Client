package org.daodao.jdbc.config;

import org.daodao.jdbc.util.Constants;

public class MongoConfig {
    
    private final String connectionString;
    private final String databaseName;
    private final String collectionName;
    
    public MongoConfig() {
        this.connectionString = Constants.MONGODB_CONNECTION_STRING;
        this.databaseName = Constants.MONGODB_DATABASE_NAME;
        this.collectionName = Constants.MONGODB_COLLECTION_NAME;
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