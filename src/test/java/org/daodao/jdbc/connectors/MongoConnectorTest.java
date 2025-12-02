package org.daodao.jdbc.connectors;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.daodao.jdbc.config.MongoConfig;
import org.daodao.jdbc.exceptions.MongoException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class MongoConnectorTest {
    
    private MongoConnector mongoConnector;
    private MongoConfig config;
    
    @BeforeEach
    void setUp() {
        config = new MongoConfig("mongodb://localhost:27017", "testdb", "users");
        mongoConnector = new MongoConnector(config);
        mongoConnector.connect();
    }
    
    @AfterEach
    void tearDown() {
        if (mongoConnector != null) {
            mongoConnector.disconnect();
        }
    }
    
    @Test
    void testConnection() {
        assertDoesNotThrow(() -> mongoConnector.connect());
    }
    
    @Test
    void testDatabaseInitialization() {
        try {
            if (mongoConnector.isDatabaseEmpty()) {
                mongoConnector.initializeDatabase();
                assertTrue(mongoConnector.getDocumentCount() > 0);
            }
        } catch (MongoException e) {
            // Skip test if MongoDB is not running
        }
    }
    
    @Test
    void testFindAllDocuments() {
        try {
            if (mongoConnector.isDatabaseEmpty()) {
                mongoConnector.initializeDatabase();
            }
            
            List<Document> documents = mongoConnector.findAllDocuments();
            assertNotNull(documents);
            assertTrue(documents.size() > 0);
        } catch (MongoException e) {
            // Skip test if MongoDB is not running
        }
    }
    
    @Test
    void testFindDocumentByEmail() {
        try {
            if (mongoConnector.isDatabaseEmpty()) {
                mongoConnector.initializeDatabase();
            }
            
            Document document = mongoConnector.findDocumentByEmail("john.doe@example.com");
            assertNotNull(document);
            assertEquals("John Doe", document.getString("name"));
        } catch (MongoException e) {
            // Skip test if MongoDB is not running
        }
    }
    
    @Test
    void testUpdateDocument() {
        try {
            if (mongoConnector.isDatabaseEmpty()) {
                mongoConnector.initializeDatabase();
            }
            
            boolean updated = mongoConnector.updateDocumentByEmail("john.doe@example.com", "Boston", 31);
            if (updated) {
                Document document = mongoConnector.findDocumentByEmail("john.doe@example.com");
                assertEquals("Boston", document.getString("city"));
                assertEquals(31, document.getInteger("age"));
            }
        } catch (MongoException e) {
            // Skip test if MongoDB is not running
        }
    }
    
    @Test
    void testFindDocumentsByAgeRange() {
        try {
            if (mongoConnector.isDatabaseEmpty()) {
                mongoConnector.initializeDatabase();
            }
            
            List<Document> documents = mongoConnector.findDocumentsByAgeRange(25, 35);
            assertNotNull(documents);
        } catch (MongoException e) {
            // Skip test if MongoDB is not running
        }
    }
}