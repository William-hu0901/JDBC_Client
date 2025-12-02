package org.daodao.jdbc.mongodb;

import com.mongodb.ReadPreference;
import com.mongodb.TransactionOptions;
import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MongoTransactionTest {
    
    private static final Logger log = LoggerFactory.getLogger(MongoTransactionTest.class);
    private static final String TEST_DATABASE = "testdb";
    private static final String ACCOUNTS_COLLECTION = "accounts";
    private static final String TRANSACTIONS_COLLECTION = "transactions";
    
    private MongoClient mongoClient;
    private MongoDatabase database;
    private MongoCollection<Document> accountsCollection;
    private MongoCollection<Document> transactionsCollection;
    
    @BeforeAll
    void setUp() {
        try {
            mongoClient = MongoClients.create("mongodb://localhost:27017");
            database = mongoClient.getDatabase(TEST_DATABASE);
            accountsCollection = database.getCollection(ACCOUNTS_COLLECTION);
            transactionsCollection = database.getCollection(TRANSACTIONS_COLLECTION);
            
            // Clean up before tests
            accountsCollection.drop();
            transactionsCollection.drop();
            
            // Create initial accounts
            List<Document> accounts = Arrays.asList(
                    new Document("accountId", "ACC001")
                            .append("owner", "Alice")
                            .append("balance", 1000.0)
                            .append("type", "checking"),
                    new Document("accountId", "ACC002")
                            .append("owner", "Bob")
                            .append("balance", 500.0)
                            .append("type", "savings")
            );
            accountsCollection.insertMany(accounts);
            
            log.info("MongoDB connection established for transaction tests");
        } catch (Exception e) {
            log.warn("MongoDB not available or transactions not supported, skipping tests: {}", e.getMessage());
            throw new RuntimeException("MongoDB not available or transactions not supported", e);
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
    void cleanTransactions() {
        transactionsCollection.drop();
    }
    
    @Test
    void testSuccessfulTransaction() {
        ClientSession session = mongoClient.startSession();
        
        try {
            session.withTransaction(() -> {
                // Debit from Alice's account
                Bson aliceFilter = Filters.eq("accountId", "ACC001");
                UpdateResult debitResult = accountsCollection.updateOne(
                        session, aliceFilter, Updates.inc("balance", -200.0)
                );
                assertEquals(1, debitResult.getModifiedCount());
                
                // Credit to Bob's account
                Bson bobFilter = Filters.eq("accountId", "ACC002");
                UpdateResult creditResult = accountsCollection.updateOne(
                        session, bobFilter, Updates.inc("balance", 200.0)
                );
                assertEquals(1, creditResult.getModifiedCount());
                
                // Record transaction
                Document transaction = new Document("fromAccount", "ACC001")
                        .append("toAccount", "ACC002")
                        .append("amount", 200.0)
                        .append("type", "transfer")
                        .append("timestamp", new Date())
                        .append("status", "completed");
                transactionsCollection.insertOne(session, transaction);
                
                return null; // Return value for transaction
            });
            
            // Verify transaction results
            Document alice = accountsCollection.find(Filters.eq("accountId", "ACC001")).first();
            Document bob = accountsCollection.find(Filters.eq("accountId", "ACC002")).first();
            Document transaction = transactionsCollection.find().first();
            
            assertEquals(800.0, alice.getDouble("balance"));
            assertEquals(700.0, bob.getDouble("balance"));
            assertNotNull(transaction);
            assertEquals(200.0, transaction.getDouble("amount"));
            
            log.info("Successfully completed transaction: {} -> {} for ${}", 
                    "ACC001", "ACC002", 200.0);
            
        } finally {
            session.close();
        }
    }
    
    @Test
    void testFailedTransactionRollback() {
        // Get initial balances
        Document initialAlice = accountsCollection.find(Filters.eq("accountId", "ACC001")).first();
        Document initialBob = accountsCollection.find(Filters.eq("accountId", "ACC002")).first();
        double aliceInitialBalance = initialAlice.getDouble("balance");
        double bobInitialBalance = initialBob.getDouble("balance");
        
        ClientSession session = mongoClient.startSession();
        
        try {
            assertThrows(Exception.class, () -> {
                session.withTransaction(() -> {
                    // Debit from Alice's account
                    accountsCollection.updateOne(
                            session, 
                            Filters.eq("accountId", "ACC001"), 
                            Updates.inc("balance", -300.0)
                    );
                    
                    // This will cause an exception - trying to credit more than exists
                    accountsCollection.updateOne(
                            session, 
                            Filters.eq("accountId", "ACC002"), 
                            Updates.inc("balance", 1000.0) // This would be fine
                    );
                    
                    // Intentionally cause an error
                    throw new RuntimeException("Intentional transaction failure");
                });
            });
            
        } finally {
            session.close();
        }
        
        // Verify rollback - balances should be unchanged
        Document aliceAfter = accountsCollection.find(Filters.eq("accountId", "ACC001")).first();
        Document bobAfter = accountsCollection.find(Filters.eq("accountId", "ACC002")).first();
        
        assertEquals(aliceInitialBalance, aliceAfter.getDouble("balance"));
        assertEquals(bobInitialBalance, bobAfter.getDouble("balance"));
        
        // No transaction should be recorded
        long transactionCount = transactionsCollection.countDocuments();
        assertEquals(0, transactionCount);
        
        log.info("Transaction rollback verified - balances unchanged");
    }
    
    @Test
    void testTransactionWithRetry() {
        ClientSession session = mongoClient.startSession();
        
        try {
            session.withTransaction(() -> {
                // Perform multiple operations in transaction
                accountsCollection.updateOne(
                        session, 
                        Filters.eq("accountId", "ACC001"), 
                        Updates.inc("balance", 100.0)
                );
                
                accountsCollection.updateOne(
                        session, 
                        Filters.eq("accountId", "ACC002"), 
                        Updates.inc("balance", -100.0)
                );
                
                Document transaction = new Document("fromAccount", "ACC002")
                        .append("toAccount", "ACC001")
                        .append("amount", 100.0)
                        .append("type", "transfer")
                        .append("timestamp", new Date());
                transactionsCollection.insertOne(session, transaction);
                
                return null;
            });
            
            // Verify results
            Document alice = accountsCollection.find(Filters.eq("accountId", "ACC001")).first();
            Document bob = accountsCollection.find(Filters.eq("accountId", "ACC002")).first();
            
            assertEquals(1100.0, alice.getDouble("balance"));
            assertEquals(400.0, bob.getDouble("balance"));
            
            log.info("Transaction with retry options completed successfully");
            
        } finally {
            session.close();
        }
    }
    
    @Test
    void testNestedTransactionOperations() {
        ClientSession session = mongoClient.startSession();
        
        try {
            session.withTransaction(() -> {
                // First operation: Update account balances
                accountsCollection.updateOne(
                        session, 
                        Filters.eq("accountId", "ACC001"), 
                        Updates.combine(
                                Updates.inc("balance", 50.0),
                                Updates.set("lastModified", new Date())
                        )
                );
                
                // Second operation: Create multiple transaction records
                List<Document> transactions = Arrays.asList(
                        new Document("accountId", "ACC001")
                                .append("type", "deposit")
                                .append("amount", 50.0)
                                .append("timestamp", new Date()),
                        new Document("accountId", "ACC001")
                                .append("type", "fee")
                                .append("amount", -2.0)
                                .append("timestamp", new Date())
                );
                transactionsCollection.insertMany(session, transactions);
                
                // Third operation: Update account type based on balance
                Document aliceAccount = accountsCollection.find(
                        session, Filters.eq("accountId", "ACC001")
                ).first();
                
                if (aliceAccount.getDouble("balance") > 1000.0) {
                    accountsCollection.updateOne(
                            session, 
                            Filters.eq("accountId", "ACC001"), 
                            Updates.set("type", "premium")
                    );
                }
                
                return null;
            });
            
            // Verify all operations completed
            Document alice = accountsCollection.find(Filters.eq("accountId", "ACC001")).first();
            assertEquals(1050.0, alice.getDouble("balance"));
            assertEquals("premium", alice.getString("type"));
            assertNotNull(alice.getDate("lastModified"));
            
            long transactionCount = transactionsCollection.countDocuments();
            assertEquals(2, transactionCount);
            
            log.info("Nested transaction operations completed successfully");
            
        } finally {
            session.close();
        }
    }
    
    @Test
    void testTransactionIsolation() {
        ClientSession session1 = mongoClient.startSession();
        ClientSession session2 = mongoClient.startSession();
        
        try {
            // Start first transaction but don't commit yet
            Thread transactionThread = new Thread(() -> {
                session1.withTransaction(() -> {
                    accountsCollection.updateOne(
                            session1, 
                            Filters.eq("accountId", "ACC001"), 
                            Updates.inc("balance", 100.0)
                    );
                    
                    // Sleep to simulate long transaction
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    
                    return null;
                });
            });
            
            transactionThread.start();
            
            // Give the first transaction a moment to start
            Thread.sleep(100);
            
            // Read with second session - should not see uncommitted changes
            Document aliceDuring = accountsCollection.find(
                    session2, Filters.eq("accountId", "ACC001")
            ).first();
            
            assertEquals(1000.0, aliceDuring.getDouble("balance"));
            
            // Wait for first transaction to complete
            transactionThread.join();
            
            // Now read again - should see committed changes
            Document aliceAfter = accountsCollection.find(
                    session2, Filters.eq("accountId", "ACC001")
            ).first();
            
            assertEquals(1100.0, aliceAfter.getDouble("balance"));
            
            log.info("Transaction isolation verified");
            
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            fail("Test interrupted");
        } finally {
            session1.close();
            session2.close();
        }
    }
}