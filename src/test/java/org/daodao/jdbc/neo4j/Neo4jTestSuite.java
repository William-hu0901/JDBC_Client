package org.daodao.jdbc.neo4j;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;
import org.junit.platform.suite.api.SuiteDisplayName;

/**
 * Neo4j Test Suite
 * 
 * This test suite orchestrates all Neo4j-related tests in the project.
 * It provides comprehensive coverage of Neo4j functionality including:
 * 
 * 1. Neo4jCRUDTest - Basic CRUD operations and database interactions
 * 2. Neo4jBasicFunctionalityTest - Core Neo4j features and Cypher queries
 * 3. Neo4jNewFeaturesTest - Latest Neo4j features and capabilities
 * 
 * Test Categories:
 * - Integration Tests: Require actual Neo4j database connection
 * - Feature Tests: Test specific Neo4j capabilities
 * - Edge Case Tests: Error handling and boundary conditions
 * 
 * Execution Order:
 * Tests are designed to be independent and can be executed in any order.
 * Each test class handles its own setup and cleanup to ensure test isolation.
 * 
 * Requirements:
 * - Neo4j server running on localhost:7687 (for integration tests)
 * - Proper configuration in application.properties
 * - All dependencies available in classpath
 * 
 * Logging:
 * Tests use SLF4J for logging with appropriate log levels for debugging
 * and monitoring test execution.
 */
@Suite
@SuiteDisplayName("Neo4j Database Test Suite")
@SelectClasses({
    Neo4jCRUDTest.class,
    Neo4jBasicFunctionalityTest.class,
    Neo4jNewFeaturesTest.class
})
public class Neo4jTestSuite {
    // This class serves as a test suite orchestrator
    // All test configuration and execution is handled by the JUnit Platform Suite
}