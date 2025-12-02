package org.daodao.jdbc.mongodb;

import com.mongodb.client.*;
import com.mongodb.client.model.*;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MongoIndexingAggregationTest {
    
    private static final Logger log = LoggerFactory.getLogger(MongoIndexingAggregationTest.class);
    private static final String TEST_DATABASE = "testdb";
    private static final String TEST_COLLECTION = "products";
    
    private MongoClient mongoClient;
    private MongoDatabase database;
    private MongoCollection<Document> collection;
    
    @BeforeAll
    void setUp() {
        try {
            mongoClient = MongoClients.create("mongodb://localhost:27017");
            database = mongoClient.getDatabase(TEST_DATABASE);
            collection = database.getCollection(TEST_COLLECTION);
            
            // Clean up before tests
            collection.drop();
            
            log.info("MongoDB connection established for indexing and aggregation tests");
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
    void prepareTestData() {
        collection.drop();
        
        // Insert sample product data
        List<Document> products = Arrays.asList(
                new Document("name", "Laptop")
                        .append("category", "Electronics")
                        .append("price", 999.99)
                        .append("brand", "Dell")
                        .append("stock", 50)
                        .append("rating", 4.5)
                        .append("tags", Arrays.asList("computer", "work", "portable")),
                new Document("name", "Smartphone")
                        .append("category", "Electronics")
                        .append("price", 699.99)
                        .append("brand", "Apple")
                        .append("stock", 100)
                        .append("rating", 4.8)
                        .append("tags", Arrays.asList("phone", "mobile", "communication")),
                new Document("name", "Office Chair")
                        .append("category", "Furniture")
                        .append("price", 299.99)
                        .append("brand", "Herman Miller")
                        .append("stock", 25)
                        .append("rating", 4.2)
                        .append("tags", Arrays.asList("furniture", "office", "comfort")),
                new Document("name", "Desk")
                        .append("category", "Furniture")
                        .append("price", 499.99)
                        .append("brand", "IKEA")
                        .append("stock", 30)
                        .append("rating", 4.0)
                        .append("tags", Arrays.asList("furniture", "office", "workspace")),
                new Document("name", "Tablet")
                        .append("category", "Electronics")
                        .append("price", 399.99)
                        .append("brand", "Samsung")
                        .append("stock", 75)
                        .append("rating", 4.3)
                        .append("tags", Arrays.asList("tablet", "mobile", "entertainment"))
        );
        
        collection.insertMany(products);
        log.info("Inserted {} test products", products.size());
    }
    
    @Test
    void testCreateSingleFieldIndex() {
        String indexName = collection.createIndex(Indexes.ascending("category"));
        
        assertNotNull(indexName);
        assertTrue(indexName.contains("category"));
        
        // Verify index exists
        for (Document index : collection.listIndexes()) {
            log.info("Index: {}", index.toJson());
        }
        
        log.info("Successfully created single field index: {}", indexName);
    }
    
    @Test
    void testCreateCompoundIndex() {
        String indexName = collection.createIndex(
                Indexes.compoundIndex(Indexes.descending("price"), Indexes.ascending("category"))
        );
        
        assertNotNull(indexName);
        
        log.info("Successfully created compound index: {}", indexName);
    }
    
    @Test
    void testCreateTextIndex() {
        String indexName = collection.createIndex(Indexes.text("name"));
        
        assertNotNull(indexName);
        
        // Test text search
        Bson textSearch = Filters.text("Laptop");
        List<Document> results = new ArrayList<>();
        collection.find(textSearch).into(results);
        
        assertEquals(1, results.size());
        assertEquals("Laptop", results.get(0).getString("name"));
        
        log.info("Successfully created text index and performed text search");
    }
    
    @Test
    void testCreateUniqueIndex() {
        String indexName = collection.createIndex(
                Indexes.ascending("name"),
                new IndexOptions().unique(true)
        );
        
        assertNotNull(indexName);
        
        // Test uniqueness constraint
        assertThrows(Exception.class, () -> {
            collection.insertOne(new Document("name", "Laptop").append("category", "Test"));
        });
        
        log.info("Successfully created unique index");
    }
    
    @Test
    void testAggregationMatch() {
        Bson match = Aggregates.match(Filters.eq("category", "Electronics"));
        List<Document> results = new ArrayList<>();
        collection.aggregate(Arrays.asList(match)).into(results);
        
        assertEquals(3, results.size());
        results.forEach(doc -> assertEquals("Electronics", doc.getString("category")));
        
        log.info("Found {} electronics products", results.size());
    }
    
    @Test
    void testAggregationGroup() {
        Bson group = Aggregates.group(
                "$category",
                Accumulators.sum("totalProducts", 1),
                Accumulators.avg("averagePrice", "$price"),
                Accumulators.max("maxPrice", "$price"),
                Accumulators.min("minPrice", "$price")
        );
        
        List<Document> results = new ArrayList<>();
        collection.aggregate(Arrays.asList(group)).into(results);
        
        assertEquals(2, results.size());
        
        Map<String, Document> categoryMap = new HashMap<>();
        results.forEach(doc -> categoryMap.put(doc.getString("_id"), doc));
        
        Document electronics = categoryMap.get("Electronics");
        assertNotNull(electronics);
        assertEquals(3, electronics.getInteger("totalProducts"));
        assertTrue(electronics.getDouble("averagePrice") > 0);
        
        log.info("Grouped results: {}", results);
    }
    
    @Test
    void testAggregationSort() {
        Bson sort = Aggregates.sort(Sorts.descending("price"));
        List<Document> results = new ArrayList<>();
        collection.aggregate(Arrays.asList(sort)).into(results);
        
        assertEquals(5, results.size());
        
        // Verify sorted order
        for (int i = 0; i < results.size() - 1; i++) {
            double currentPrice = results.get(i).getDouble("price");
            double nextPrice = results.get(i + 1).getDouble("price");
            assertTrue(currentPrice >= nextPrice);
        }
        
        log.info("Products sorted by price (descending)");
    }
    
    @Test
    void testAggregationLimit() {
        Bson sort = Aggregates.sort(Sorts.descending("rating"));
        Bson limit = Aggregates.limit(3);
        
        List<Document> results = new ArrayList<>();
        collection.aggregate(Arrays.asList(sort, limit)).into(results);
        
        assertEquals(3, results.size());
        
        // Verify these are the top 3 rated products
        for (int i = 0; i < results.size() - 1; i++) {
            double currentRating = results.get(i).getDouble("rating");
            double nextRating = results.get(i + 1).getDouble("rating");
            assertTrue(currentRating >= nextRating);
        }
        
        log.info("Retrieved top 3 rated products");
    }
    
    @Test
    void testAggregationProject() {
        Bson project = Aggregates.project(
                Projections.fields(
                        Projections.include("name", "price"),
                        Projections.computed("priceWithTax", new Document("$multiply", Arrays.asList("$price", 1.1))),
                        Projections.excludeId()
                )
        );
        
        List<Document> results = new ArrayList<>();
        collection.aggregate(Arrays.asList(project)).into(results);
        
        assertEquals(5, results.size());
        
        Document firstProduct = results.get(0);
        assertNotNull(firstProduct.getString("name"));
        assertNotNull(firstProduct.getDouble("price"));
        assertNotNull(firstProduct.getDouble("priceWithTax"));
        assertNull(firstProduct.get("_id"));
        
        // Verify tax calculation
        double expectedPriceWithTax = firstProduct.getDouble("price") * 1.1;
        assertEquals(expectedPriceWithTax, firstProduct.getDouble("priceWithTax"), 0.01);
        
        log.info("Successfully projected products with tax calculation");
    }
    
    @Test
    void testAggregationUnwind() {
        // Add products with multiple tags
        collection.updateMany(
                Filters.exists("tags"),
                Updates.set("tags", Arrays.asList("popular", "sale", "new"))
        );
        
        Bson unwind = Aggregates.unwind("$tags");
        List<Document> results = new ArrayList<>();
        collection.aggregate(Arrays.asList(unwind)).into(results);
        
        // Each product should be duplicated for each tag
        assertEquals(15, results.size()); // 5 products * 3 tags each
        
        log.info("Unwound tags array into {} documents", results.size());
    }
    
    @Test
    void testComplexAggregationPipeline() {
        Bson match = Aggregates.match(Filters.gte("rating", 4.0));
        Bson group = Aggregates.group(
                "$category",
                Accumulators.sum("count", 1),
                Accumulators.avg("avgRating", "$rating"),
                Accumulators.sum("totalValue", new Document("$multiply", Arrays.asList("$price", "$stock")))
        );
        Bson sort = Aggregates.sort(Sorts.descending("totalValue"));
        Bson project = Aggregates.project(
                Projections.fields(
                        Projections.computed("category", "$_id"),
                        Projections.include("count", "avgRating", "totalValue"),
                        Projections.excludeId()
                )
        );
        
        List<Document> results = new ArrayList<>();
        collection.aggregate(Arrays.asList(match, group, sort, project)).into(results);
        
        assertFalse(results.isEmpty());
        
        Document topCategory = results.get(0);
        assertNotNull(topCategory.getString("category"));
        assertTrue(topCategory.getInteger("count") > 0);
        assertTrue(topCategory.getDouble("avgRating") >= 4.0);
        
        log.info("Complex aggregation results: {}", results);
    }
    
    @Test
    void testAggregationWithLookup() {
        // Create orders collection for join
        MongoCollection<Document> ordersCollection = database.getCollection("orders");
        ordersCollection.drop();
        
        List<Document> orders = Arrays.asList(
                new Document("productId", 1)
                        .append("quantity", 2)
                        .append("total", 1999.98)
                        .append("date", new Date()),
                new Document("productId", 2)
                        .append("quantity", 1)
                        .append("total", 699.99)
                        .append("date", new Date())
        );
        ordersCollection.insertMany(orders);
        
        // Add product IDs to products
        collection.updateMany(Filters.exists("name"), Updates.set("productId", 1));
        collection.updateOne(Filters.eq("name", "Smartphone"), Updates.set("productId", 2));
        
        Bson lookup = Aggregates.lookup(
                "orders",
                "productId",
                "productId",
                "productOrders"
        );
        
        List<Document> results = new ArrayList<>();
        collection.aggregate(Arrays.asList(lookup)).into(results);
        
        // Verify some products have orders
        boolean hasOrders = results.stream()
                .anyMatch(doc -> {
                    @SuppressWarnings("unchecked")
                    List<Document> productOrders = (List<Document>) doc.get("productOrders");
                    return productOrders != null && !productOrders.isEmpty();
                });
        
        assertTrue(hasOrders);
        
        // Clean up
        ordersCollection.drop();
        
        log.info("Successfully performed lookup aggregation");
    }
}