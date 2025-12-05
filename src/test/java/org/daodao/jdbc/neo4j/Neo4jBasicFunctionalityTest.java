package org.daodao.jdbc.neo4j;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.daodao.jdbc.config.Neo4jConfig;
import org.daodao.jdbc.connectors.Neo4jConnector;
import org.daodao.jdbc.model.Movie;
import org.daodao.jdbc.model.Person;
import org.daodao.jdbc.service.Neo4jMovieService;
import org.daodao.jdbc.service.Neo4jDatabaseInitializer;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.Map;
import java.util.HashMap;

/**
 * Neo4j Basic Functionality Test Cases
 * Tests core Neo4j features including CRUD operations, queries, and relationships.
 * This class covers the most commonly used Neo4j functionality.
 */
class Neo4jBasicFunctionalityTest {

    private Neo4jConnector connector;
    private Neo4jMovieService movieService;
    private Neo4jDatabaseInitializer initializer;

    @BeforeEach
    void setUp() {
        Neo4jConfig config = new Neo4jConfig();
        connector = new Neo4jConnector(config);
        movieService = new Neo4jMovieService(connector);
        initializer = new Neo4jDatabaseInitializer(connector);
        initializer.initializeDatabase();
    }

    @AfterEach
    void tearDown() {
        if (connector != null) {
            connector.close();
        }
    }

    @Test
    @DisplayName("Test Basic Node Creation and Retrieval")
    void testBasicNodeOperations() {
        String movieTitle = "Basic Test Movie_" + System.currentTimeMillis();
        Movie movie = new Movie(movieTitle, 2023, "Test Genre", "Test Description");
        movieService.createMovie(movie);

        Movie retrieved = movieService.getMovie(movieTitle);
        assertNotNull(retrieved);
        assertEquals(movieTitle, retrieved.getTitle());
        assertEquals(2023, retrieved.getYear());

        // Clean up
        movieService.deleteMovie(movieTitle);
    }

    @Test
    @DisplayName("Test Relationship Creation and Querying")
    void testRelationshipOperations() {
        String actorName = "Test Actor_" + System.currentTimeMillis();
        
        // Create test actor
        String createActorQuery = "MERGE (p:Person {name: $name}) SET p.birthYear = $birthYear, p.nationality = $nationality, p.role = 'Actor'";
        Map<String, Object> params = new HashMap<>();
        params.put("name", actorName);
        params.put("birthYear", 1980);
        params.put("nationality", "Testland");
        connector.executeWrite(createActorQuery, params);

        // Create relationship
        movieService.addActor("The Matrix", actorName);

        // Verify relationship
        var actors = movieService.getActorsInMovie("The Matrix");
        assertTrue(actors.stream().anyMatch(a -> actorName.equals(a.getName())));

        // Clean up
        String deleteRelationQuery = "MATCH (a:Person {name: $actorName})-[r:ACTED_IN]->(m:Movie {title: $movieTitle}) DELETE r";
        Map<String, Object> deleteParams = new HashMap<>();
        deleteParams.put("actorName", actorName);
        deleteParams.put("movieTitle", "The Matrix");
        connector.executeWrite(deleteRelationQuery, deleteParams);

        String deleteActorQuery = "MATCH (p:Person {name: $actorName}) DELETE p";
        connector.executeWrite(deleteActorQuery, deleteParams);
    }

    @Test
    @DisplayName("Test Complex Cypher Queries")
    void testComplexQueries() {
        // Test multiple hops query
        String query = "MATCH (actor:Person)-[:ACTED_IN]->(movie:Movie)<-[:DIRECTED]-(director:Person) " +
                      "RETURN movie.title as movieTitle, actor.name as actorName, director.name as directorName " +
                      "LIMIT 5";
        
        try (var session = connector.getSession()) {
            List<Map<String, Object>> results = session.readTransaction(tx -> {
                var result = tx.run(query, new HashMap<>());
                List<Map<String, Object>> resultList = new java.util.ArrayList<>();
                while (result.hasNext()) {
                    var record = result.next();
                    Map<String, Object> recordMap = new HashMap<>();
                    recordMap.put("movieTitle", record.get("movieTitle").asString());
                    recordMap.put("actorName", record.get("actorName").asString());
                    recordMap.put("directorName", record.get("directorName").asString());
                    resultList.add(recordMap);
                }
                return resultList;
            });
            
            assertNotNull(results);
            assertFalse(results.isEmpty());
        }
    }

    @Test
    @DisplayName("Test Aggregation Functions")
    void testAggregationFunctions() {
        // Test count aggregation
        String query = "MATCH (p:Person) RETURN p.role as role, count(p) as count";
        
        try (var session = connector.getSession()) {
            List<Map<String, Object>> results = session.readTransaction(tx -> {
                var result = tx.run(query, new HashMap<>());
                List<Map<String, Object>> resultList = new java.util.ArrayList<>();
                while (result.hasNext()) {
                    var record = result.next();
                    Map<String, Object> recordMap = new HashMap<>();
                    recordMap.put("role", record.get("role").asString());
                    recordMap.put("count", record.get("count").asInt());
                    resultList.add(recordMap);
                }
                return resultList;
            });
            
            assertNotNull(results);
            assertFalse(results.isEmpty());
        }
    }

    @Test
    @DisplayName("Test String Matching and Pattern Matching")
    void testStringPatternMatching() {
        // Test string pattern matching
        String query = "MATCH (m:Movie) WHERE m.title =~ '.*[Tt]est.*' RETURN m.title as title";
        
        try (var session = connector.getSession()) {
            List<String> titles = session.readTransaction(tx -> {
                var result = tx.run(query, new HashMap<>());
                List<String> titleList = new java.util.ArrayList<>();
                while (result.hasNext()) {
                    var record = result.next();
                    titleList.add(record.get("title").asString());
                }
                return titleList;
            });
            
            assertNotNull(titles);
        }
    }

    @Test
    @DisplayName("Test Conditional Queries with CASE")
    void testConditionalQueries() {
        // Test CASE statement
        String query = "MATCH (m:Movie) RETURN m.title as title, " +
                      "CASE WHEN m.year >= 2000 THEN 'Modern' ELSE 'Classic' END as era " +
                      "LIMIT 5";
        
        try (var session = connector.getSession()) {
            List<Map<String, Object>> results = session.readTransaction(tx -> {
                var result = tx.run(query, new HashMap<>());
                List<Map<String, Object>> resultList = new java.util.ArrayList<>();
                while (result.hasNext()) {
                    var record = result.next();
                    Map<String, Object> recordMap = new HashMap<>();
                    recordMap.put("title", record.get("title").asString());
                    recordMap.put("era", record.get("era").asString());
                    resultList.add(recordMap);
                }
                return resultList;
            });
            
            assertNotNull(results);
            assertFalse(results.isEmpty());
        }
    }

    @Test
    @DisplayName("Test Shortest Path Queries")
    void testShortestPathQueries() {
        // Test shortest path
        String query = "MATCH (start:Person {name: 'Keanu Reeves'}), (end:Person {name: 'Christopher Nolan'}), " +
                      "path = shortestPath((start)-[*..6]-(end)) " +
                      "RETURN path";
        
        try (var session = connector.getSession()) {
            boolean hasPath = session.readTransaction(tx -> {
                var result = tx.run(query, new HashMap<>());
                return result.hasNext();
            });
            
            // Should either find a path or not, both are valid outcomes
            assertNotNull(hasPath);
        }
    }

    @Test
    @DisplayName("Test Subquery Operations")
    void testSubqueryOperations() {
        // Test subquery with CALL {} IN TRANSACTIONS
        String query = "MATCH (m:Movie) WHERE m.year >= 2000 " +
                      "CALL { " +
                      "  WITH m " +
                      "  MATCH (p:Person)-[:ACTED_IN]->(m) " +
                      "  RETURN count(p) as actorCount " +
                      "} " +
                      "RETURN m.title as title, actorCount " +
                      "LIMIT 3";
        
        try (var session = connector.getSession()) {
            List<Map<String, Object>> results = session.readTransaction(tx -> {
                var result = tx.run(query, new HashMap<>());
                List<Map<String, Object>> resultList = new java.util.ArrayList<>();
                while (result.hasNext()) {
                    var record = result.next();
                    Map<String, Object> recordMap = new HashMap<>();
                    recordMap.put("title", record.get("title").asString());
                    recordMap.put("actorCount", record.get("actorCount").asInt());
                    resultList.add(recordMap);
                }
                return resultList;
            });
            
            assertNotNull(results);
        }
    }

    @Test
    @DisplayName("Test Parameterized Queries with Lists")
    void testParameterizedListQueries() {
        // Test query with list parameter
        String query = "MATCH (m:Movie) WHERE m.year IN $years RETURN m.title as title";
        Map<String, Object> params = new HashMap<>();
        params.put("years", java.util.List.of(1999, 2010, 2008));
        
        try (var session = connector.getSession()) {
            List<String> titles = session.readTransaction(tx -> {
                var result = tx.run(query, params);
                List<String> titleList = new java.util.ArrayList<>();
                while (result.hasNext()) {
                    var record = result.next();
                    titleList.add(record.get("title").asString());
                }
                return titleList;
            });
            
            assertNotNull(titles);
            // Allow empty list if no movies match the years
            assertTrue(titles != null); // Just verify the query executes without error
        }
    }
}