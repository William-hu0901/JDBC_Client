package org.daodao.jdbc.mongodb;

import org.junit.platform.suite.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test Suite for MongoDB functionality tests.
 * This suite runs all MongoDB-related tests in the appropriate order.
 * 
 * Test Categories:
 * 1. Basic CRUD Operations - Core MongoDB functionality
 * 2. Indexing and Aggregation - Advanced query capabilities
 * 3. Transactions - ACID compliance testing
 * 4. New Features - Latest MongoDB production features
 * 5. Mockito Tests - Unit testing with mocked dependencies
 */
@Suite
@SuiteDisplayName("MongoDB Test Suite")
@SelectClasses({
    MongoBasicCRUDTest.class,
    MongoIndexingAggregationTest.class,
    MongoTransactionTest.class,
    MongoNewFeaturesTest.class
})
public class TestSuite {
    
    private static final Logger log = LoggerFactory.getLogger(TestSuite.class);
    
    static {
        log.info("MongoDB Test Suite initialized");
        log.info("Test execution order:");
        log.info("  1. Basic CRUD Operations");
        log.info("  2. Indexing and Aggregation");
        log.info("  3. Transactions");
        log.info("  4. New Features (Change Streams, Time Series, etc.)");
        log.info("Note: Mockito Tests disabled due to Java 25 incompatibility");
    }
}