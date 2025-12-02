package org.daodao.jdbc.mongodb;

import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.result.*;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MongoBasicCRUDTest {
    
    private static final Logger log = LoggerFactory.getLogger(MongoBasicCRUDTest.class);
    private static final String TEST_DATABASE = "testdb";
    private static final String TEST_COLLECTION = "users";
    
    private MongoClient mongoClient;
    private MongoDatabase database;
    private MongoCollection<Document> collection;
    
    @BeforeAll
    void setUp() {
        try {
            mongoClient = MongoClients.create("mongodb://localhost:27017/?connectTimeoutMS=5000&serverSelectionTimeoutMS=5000");
            database = mongoClient.getDatabase(TEST_DATABASE);
            collection = database.getCollection(TEST_COLLECTION);
            
            // Clean up before tests
            collection.drop();
            
            log.info("MongoDB connection established for basic CRUD tests");
        } catch (Exception e) {
            log.warn("MongoDB not available, skipping tests: {}", e.getMessage());
            throw new RuntimeException("MongoDB not available", e);
        }
    }
    
    @AfterAll
    void tearDown() {
        if (mongoClient != null) {
            mongoClient.close();
            log.info("MongoDB connection closed");
        }
    }
    
    @BeforeEach
    void cleanCollection() {
        collection.drop();
    }
    
    @Test
    void testInsertSingleDocument() {
        Document user = new Document("name", "John Doe")
                .append("email", "john@example.com")
                .append("age", 30)
                .append("active", true);
        
        InsertOneResult result = collection.insertOne(user);
        
        assertNotNull(result);
        assertNotNull(result.getInsertedId());
        assertEquals(1L, collection.countDocuments());
        
        log.info("Successfully inserted single document with ID: {}", result.getInsertedId());
    }
    
    @Test
    void testInsertMultipleDocuments() {
        List<Document> users = Arrays.asList(
                new Document("name", "Alice").append("email", "alice@example.com").append("age", 25),
                new Document("name", "Bob").append("email", "bob@example.com").append("age", 35),
                new Document("name", "Charlie").append("email", "charlie@example.com").append("age", 28)
        );
        
        InsertManyResult result = collection.insertMany(users);
        
        assertNotNull(result);
        assertEquals(3, result.getInsertedIds().size());
        assertEquals(3L, collection.countDocuments());
        
        log.info("Successfully inserted {} documents", result.getInsertedIds().size());
    }
    
    @Test
    void testFindAllDocuments() {
        // Insert test data
        collection.insertMany(Arrays.asList(
                new Document("name", "User1").append("email", "user1@example.com"),
                new Document("name", "User2").append("email", "user2@example.com")
        ));
        
        List<Document> documents = new ArrayList<>();
        collection.find().into(documents);
        
        assertEquals(2, documents.size());
        log.info("Found {} documents", documents.size());
    }
    
    @Test
    void testFindWithFilter() {
        // Insert test data
        collection.insertMany(Arrays.asList(
                new Document("name", "Alice").append("age", 25).append("department", "IT"),
                new Document("name", "Bob").append("age", 35).append("department", "HR"),
                new Document("name", "Charlie").append("age", 30).append("department", "IT")
        ));
        
        Bson filter = Filters.eq("department", "IT");
        List<Document> itDocuments = new ArrayList<>();
        collection.find(filter).into(itDocuments);
        
        assertEquals(2, itDocuments.size());
        log.info("Found {} documents in IT department", itDocuments.size());
    }
    
    @Test
    void testFindWithComplexFilter() {
        // Insert test data
        collection.insertMany(Arrays.asList(
                new Document("name", "Alice").append("age", 25).append("salary", 50000),
                new Document("name", "Bob").append("age", 35).append("salary", 70000),
                new Document("name", "Charlie").append("age", 30).append("salary", 60000)
        ));
        
        Bson filter = Filters.and(
                Filters.gte("age", 25),
                Filters.lte("age", 30),
                Filters.gte("salary", 50000)
        );
        
        List<Document> documents = new ArrayList<>();
        collection.find(filter).into(documents);
        
        assertEquals(2, documents.size());
        log.info("Found {} documents matching complex filter", documents.size());
    }
    
    @Test
    void testUpdateSingleDocument() {
        // Insert test document
        collection.insertOne(new Document("name", "Alice").append("age", 25).append("status", "active"));
        
        Bson filter = Filters.eq("name", "Alice");
        Bson update = Updates.combine(
                Updates.set("age", 26),
                Updates.set("status", "inactive"),
                Updates.currentDate("lastUpdated")
        );
        
        UpdateResult result = collection.updateOne(filter, update);
        
        assertEquals(1, result.getModifiedCount());
        
        Document updated = collection.find(filter).first();
        assertEquals(26, updated.getInteger("age"));
        assertEquals("inactive", updated.getString("status"));
        assertNotNull(updated.getDate("lastUpdated"));
        
        log.info("Successfully updated document, modified count: {}", result.getModifiedCount());
    }
    
    @Test
    void testUpdateMultipleDocuments() {
        // Insert test documents
        collection.insertMany(Arrays.asList(
                new Document("department", "IT").append("budget", 100000),
                new Document("department", "HR").append("budget", 80000),
                new Document("department", "IT").append("budget", 120000)
        ));
        
        Bson filter = Filters.eq("department", "IT");
        Bson update = Updates.mul("budget", 1.1); // Increase by 10%
        
        UpdateResult result = collection.updateMany(filter, update);
        
        assertEquals(2, result.getModifiedCount());
        
        List<Document> itDocs = new ArrayList<>();
        collection.find(filter).into(itDocs);
        
        itDocs.forEach(doc -> {
            Double budget = doc.getDouble("budget");
            assertTrue(budget > 100000);
        });
        
        log.info("Successfully updated {} documents", result.getModifiedCount());
    }
    
    @Test
    void testReplaceDocument() {
        // Insert test document
        collection.insertOne(new Document("name", "Alice").append("age", 25));
        
        Bson filter = Filters.eq("name", "Alice");
        Document replacement = new Document("name", "Alice Smith")
                .append("age", 26)
                .append("email", "alice@example.com")
                .append("department", "Engineering");
        
        ReplaceOptions options = new ReplaceOptions().upsert(true);
        UpdateResult result = collection.replaceOne(filter, replacement, options);
        
        assertEquals(1, result.getModifiedCount());
        
        // Use the new name for finding the replaced document
        Document replaced = collection.find(Filters.eq("name", "Alice Smith")).first();
        assertNotNull(replaced);
        assertEquals("Alice Smith", replaced.getString("name"));
        assertEquals("alice@example.com", replaced.getString("email"));
        assertEquals("Engineering", replaced.getString("department"));
        
        log.info("Successfully replaced document");
    }
    
    @Test
    void testDeleteSingleDocument() {
        // Insert test documents
        collection.insertMany(Arrays.asList(
                new Document("name", "Alice").append("status", "active"),
                new Document("name", "Bob").append("status", "inactive")
        ));
        
        Bson filter = Filters.eq("status", "inactive");
        DeleteResult result = collection.deleteOne(filter);
        
        assertEquals(1, result.getDeletedCount());
        assertEquals(1L, collection.countDocuments());
        
        log.info("Successfully deleted document, deleted count: {}", result.getDeletedCount());
    }
    
    @Test
    void testDeleteMultipleDocuments() {
        // Insert test documents
        collection.insertMany(Arrays.asList(
                new Document("status", "active"),
                new Document("status", "inactive"),
                new Document("status", "inactive"),
                new Document("status", "active")
        ));
        
        Bson filter = Filters.eq("status", "inactive");
        DeleteResult result = collection.deleteMany(filter);
        
        assertEquals(2, result.getDeletedCount());
        assertEquals(2L, collection.countDocuments());
        
        log.info("Successfully deleted {} documents", result.getDeletedCount());
    }
    
    @Test
    void testCountDocuments() {
        // Insert test documents
        collection.insertMany(Arrays.asList(
                new Document("category", "electronics"),
                new Document("category", "electronics"),
                new Document("category", "books")
        ));
        
        long totalCount = collection.countDocuments();
        long electronicsCount = collection.countDocuments(Filters.eq("category", "electronics"));
        
        assertEquals(3, totalCount);
        assertEquals(2, electronicsCount);
        
        log.info("Total documents: {}, Electronics documents: {}", totalCount, electronicsCount);
    }
    
    @Test
    void testDistinctValues() {
        // Insert test documents
        collection.insertMany(Arrays.asList(
                new Document("name", "Alice").append("department", "IT"),
                new Document("name", "Bob").append("department", "HR"),
                new Document("name", "Charlie").append("department", "IT"),
                new Document("name", "David").append("department", "Finance")
        ));
        
        DistinctIterable<String> distinctDepartments = collection.distinct("department", String.class);
        List<String> departments = new ArrayList<>();
        distinctDepartments.into(departments);
        
        assertEquals(3, departments.size());
        assertTrue(departments.contains("IT"));
        assertTrue(departments.contains("HR"));
        assertTrue(departments.contains("Finance"));
        
        log.info("Found distinct departments: {}", departments);
    }
}