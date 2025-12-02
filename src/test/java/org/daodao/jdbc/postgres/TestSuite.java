package org.daodao.jdbc.postgres;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;
import org.junit.platform.suite.api.SuiteDisplayName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test Suite for PostgreSQL functionality tests.
 * 
 * This suite orchestrates all PostgreSQL-related tests including:
 * - Basic CRUD operations
 * - Indexing and performance features
 * - Transaction handling
 * - Latest PostgreSQL features
 * - Unit tests with Mockito
 * - Simple infrastructure tests
 */
@Suite
@SuiteDisplayName("PostgreSQL Test Suite")
@SelectClasses({
    PostgresBasicCRUDTest.class,
    PostgresIndexingTest.class,
    PostgresTransactionTest.class,
    PostgresNewFeaturesTest.class,
    PostgresConnectorMockitoTest.class,
    PostgresSimpleTest.class
})
public class TestSuite {
    
    private static final Logger log = LoggerFactory.getLogger(TestSuite.class);
    
    static {
        log.info("PostgreSQL Test Suite initialized");
        log.info("Starting comprehensive PostgreSQL functionality testing");
        log.info("Test classes included:");
        log.info("  - PostgresBasicCRUDTest: Basic CRUD operations");
        log.info("  - PostgresIndexingTest: Index creation and management");
        log.info("  - PostgresTransactionTest: ACID transaction handling");
        log.info("  - PostgresNewFeaturesTest: Latest PostgreSQL production features");
        log.info("  - PostgresConnectorMockitoTest: Unit tests with Mockito");
        log.info("  - PostgresSimpleTest: Basic infrastructure tests");
    }
}