package org.daodao.jdbc.connectors;

import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.daodao.jdbc.config.MongoConfig;
import org.daodao.jdbc.exceptions.MongoException;
import org.daodao.jdbc.util.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MongoConnector {
    
    private static final Logger logger = LoggerFactory.getLogger(MongoConnector.class);
    
    private final MongoConfig config;
    private MongoClient mongoClient;
    private MongoDatabase database;
    private MongoCollection<Document> collection;
    
    public MongoConnector() {
        this.config = new MongoConfig();
    }
    
    public MongoConnector(MongoConfig config) {
        this.config = config;
    }
    
    public void connect() {
        try {
            mongoClient = MongoClients.create(config.getConnectionString());
            database = mongoClient.getDatabase(config.getDatabaseName());
            collection = database.getCollection(config.getCollectionName());
            logger.info("Connected to MongoDB successfully");
        } catch (Exception e) {
            throw new MongoException("Failed to connect to MongoDB", e);
        }
    }
    
    public void disconnect() {
        if (mongoClient != null) {
            mongoClient.close();
            logger.info("Disconnected from MongoDB");
        }
    }
    
    public boolean isDatabaseEmpty() {
        try {
            long count = collection.countDocuments();
            return count == 0;
        } catch (Exception e) {
            throw new MongoException("Failed to check if database is empty", e);
        }
    }
    
    public void initializeDatabase() {
        try {
            List<Document> documents = new ArrayList<>();
            
            documents.add(new Document("name", "John Doe")
                    .append("email", "john.doe@example.com")
                    .append("age", 30)
                    .append("city", "New York"));
            
            documents.add(new Document("name", "Jane Smith")
                    .append("email", "jane.smith@example.com")
                    .append("age", 25)
                    .append("city", "Los Angeles"));
            
            documents.add(new Document("name", "Bob Johnson")
                    .append("email", "bob.johnson@example.com")
                    .append("age", 35)
                    .append("city", "Chicago"));
            
            collection.insertMany(documents);
            
            collection.createIndex(new Document("email", 1));
            collection.createIndex(new Document("age", 1));
            
            logger.info("Database initialized with sample data and indexes");
        } catch (Exception e) {
            throw new MongoException("Failed to initialize database", e);
        }
    }
    
    public List<Document> findAllDocuments() {
        try {
            List<Document> documents = new ArrayList<>();
            collection.find().into(documents);
            return documents;
        } catch (Exception e) {
            throw new MongoException("Failed to find all documents", e);
        }
    }
    
    public Document findDocumentByEmail(String email) {
        try {
            Bson filter = Filters.eq("email", email);
            return collection.find(filter).first();
        } catch (Exception e) {
            throw new MongoException("Failed to find document by email: " + email, e);
        }
    }
    
    public List<Document> findDocumentsByAgeRange(int minAge, int maxAge) {
        try {
            Bson filter = Filters.and(
                    Filters.gte("age", minAge),
                    Filters.lte("age", maxAge)
            );
            List<Document> documents = new ArrayList<>();
            collection.find(filter).into(documents);
            return documents;
        } catch (Exception e) {
            throw new MongoException("Failed to find documents by age range", e);
        }
    }
    
    public boolean updateDocumentByEmail(String email, String newCity, int newAge) {
        try {
            Bson filter = Filters.eq("email", email);
            Bson update = Updates.combine(
                    Updates.set("city", newCity),
                    Updates.set("age", newAge)
            );
            UpdateResult result = collection.updateOne(filter, update);
            return result.getModifiedCount() > 0;
        } catch (Exception e) {
            throw new MongoException("Failed to update document by email: " + email, e);
        }
    }
    
    public boolean deleteDocumentByEmail(String email) {
        try {
            Bson filter = Filters.eq("email", email);
            com.mongodb.client.result.DeleteResult result = collection.deleteOne(filter);
            return result.getDeletedCount() > 0;
        } catch (Exception e) {
            throw new MongoException("Failed to delete document by email: " + email, e);
        }
    }
    
    public long getDocumentCount() {
        try {
            return collection.countDocuments();
        } catch (Exception e) {
            throw new MongoException("Failed to get document count", e);
        }
    }
}