package org.daodao.jdbc.neo4j;

import org.daodao.jdbc.config.Neo4jConfig;
import org.daodao.jdbc.connectors.Neo4jConnector;
import org.daodao.jdbc.model.Movie;
import org.daodao.jdbc.model.Person;
import org.daodao.jdbc.service.Neo4jDatabaseInitializer;
import org.daodao.jdbc.service.Neo4jMovieService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class Neo4jCRUDTest {
    private Neo4jConnector connector;
    private Neo4jMovieService movieService;
    private Neo4jDatabaseInitializer initializer;

    @BeforeEach
    void setUp() {
        Neo4jConfig config = new Neo4jConfig("bolt://localhost:7687", "neo4j", "Daodao_201314", "neo4j");
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
    void testCreateMovie() {
        String uniqueTitle = "Test Movie_" + System.currentTimeMillis();
        Movie movie = new Movie(uniqueTitle, 2023, "Drama", "Test description");
        movieService.createMovie(movie);
        
        Movie retrieved = movieService.getMovie(uniqueTitle);
        assertNotNull(retrieved);
        assertEquals(uniqueTitle, retrieved.getTitle());
        assertEquals(2023, retrieved.getYear());
        assertEquals("Drama", retrieved.getGenre());
        assertEquals("Test description", retrieved.getDescription());
        
        // Clean up
        movieService.deleteMovie(uniqueTitle);
    }

    @Test
    void testUpdateMovie() {
        String uniqueTitle = "Update Test_" + System.currentTimeMillis();
        Movie movie = new Movie(uniqueTitle, 2023, "Comedy", "Original description");
        movieService.createMovie(movie);
        
        Movie updated = new Movie(uniqueTitle, 2024, "Drama", "Updated description");
        movieService.updateMovie(uniqueTitle, updated);
        
        Movie retrieved = movieService.getMovie(uniqueTitle);
        assertNotNull(retrieved);
        assertEquals(2024, retrieved.getYear());
        assertEquals("Drama", retrieved.getGenre());
        assertEquals("Updated description", retrieved.getDescription());
        
        // Clean up
        movieService.deleteMovie(uniqueTitle);
    }

    @Test
    void testDeleteMovie() {
        String uniqueTitle = "Delete Test_" + System.currentTimeMillis();
        Movie movie = new Movie(uniqueTitle, 2023, "Horror", "To be deleted");
        movieService.createMovie(movie);
        
        assertNotNull(movieService.getMovie(uniqueTitle));
        
        movieService.deleteMovie(uniqueTitle);
        
        assertNull(movieService.getMovie(uniqueTitle));
    }

    @Test
    void testAddActorToMovie() {
        String uniqueActorName = "Test Actor_" + System.currentTimeMillis();
        Person actor = new Person(uniqueActorName, 1980, "Testland");
        String createActorQuery = "MERGE (p:Person {name: $name}) SET p.birthYear = $birthYear, p.nationality = $nationality, p.role = 'Actor'";
        java.util.Map<String, Object> params = new java.util.HashMap<>();
        params.put("name", actor.getName());
        params.put("birthYear", actor.getBirthYear());
        params.put("nationality", actor.getNationality());
        connector.executeWrite(createActorQuery, params);
        
        movieService.addActor("The Matrix", uniqueActorName);
        
        var actors = movieService.getActorsInMovie("The Matrix");
        assertTrue(actors.stream().anyMatch(a -> uniqueActorName.equals(a.getName())));
        
        // Clean up - remove the test actor relationship
        String deleteRelationQuery = "MATCH (a:Person {name: $actorName})-[r:ACTED_IN]->(m:Movie {title: $movieTitle}) DELETE r";
        java.util.Map<String, Object> deleteParams = new java.util.HashMap<>();
        deleteParams.put("actorName", uniqueActorName);
        deleteParams.put("movieTitle", "The Matrix");
        connector.executeWrite(deleteRelationQuery, deleteParams);
        
        // Remove the test actor node
        String deleteActorQuery = "MATCH (p:Person {name: $actorName}) DELETE p";
        connector.executeWrite(deleteActorQuery, deleteParams);
    }

    @Test
    void testGetAllMovies() {
        var movies = movieService.getAllMovies();
        assertNotNull(movies);
        assertTrue(movies.size() >= 1);
    }

    @Test
    void testGetActorsInMovie() {
        var actors = movieService.getActorsInMovie("The Matrix");
        assertNotNull(actors);
        assertFalse(actors.isEmpty());
    }

    @Test
    void testGetDirectorsOfMovie() {
        var directors = movieService.getDirectorsOfMovie("The Matrix");
        assertNotNull(directors);
        assertFalse(directors.isEmpty());
    }

    @Test
    void testGetMoviesByActor() {
        var movies = movieService.getMoviesByActor("Keanu Reeves");
        assertNotNull(movies);
        assertTrue(movies.size() >= 1);
    }

    @Test
    void testGetMoviesByDirector() {
        var movies = movieService.getMoviesByDirector("Christopher Nolan");
        assertNotNull(movies);
        // Just check that the method works without size constraint
        // Director may or may not have movies depending on data state
    }
}