package org.daodao.jdbc.concurrent;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

/**
 * Test suite to run concurrent database tests in specific order:
 * 1. BasicConcurrentTest - Quick validation
 * 2. SimplifiedConcurrentTest - Medium scale test
 * 3. ConcurrentDatabaseTest - Full comprehensive test
 */
@Suite
@SelectClasses({
    BasicConcurrentTest.class,
    SimplifiedConcurrentTest.class,
    ConcurrentDatabaseTest.class
})
public class TestSuite {
    // This class is a test suite and should be run as a JUnit test
}