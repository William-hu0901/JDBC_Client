package org.daodao.jdbc.neo4j;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.daodao.jdbc.connectors.Neo4jConnector;
import org.daodao.jdbc.model.Movie;
import org.daodao.jdbc.service.Neo4jMovieService;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * Neo4j Mockito Simple Unit Tests (Java21 Compatible)
 * 
 * This class provides simplified unit tests using Mockito that are compatible with Java21.
 * It avoids complex mocking scenarios that might cause Byte Buddy compatibility issues.
 * 
 * Test categories:
 * - Basic service method validation
 * - Simple mock verification
 * - Constructor validation
 * - Exception handling
 */
@ExtendWith(MockitoExtension.class)
class Neo4jMockitoSimpleTest {

    @Mock
    private Neo4jConnector mockConnector;

    private Neo4jMovieService movieService;

    @BeforeEach
    void setUp() {
        movieService = new Neo4jMovieService(mockConnector);
    }

    @Test
    @DisplayName("Test Movie Creation with Mock Connector")
    void testCreateMovieWithMock() {
        // Setup
        Movie movie = new Movie("Simple Test Movie", 2023, "Test Genre", "Test Description");
        
        // Test - should not throw exception
        assertDoesNotThrow(() -> movieService.createMovie(movie));
        
        // Verify that executeWrite was called
        verify(mockConnector, times(1)).executeWrite(anyString(), any());
    }

    @Test
    @DisplayName("Test Movie Update with Mock")
    void testUpdateMovieWithMock() {
        // Setup
        String originalTitle = "Original Movie";
        Movie updatedMovie = new Movie(originalTitle, 2024, "Updated Genre", "Updated Description");
        
        // Test
        assertDoesNotThrow(() -> movieService.updateMovie(originalTitle, updatedMovie));
        
        // Verify
        verify(mockConnector, times(1)).executeWrite(anyString(), any());
    }

    @Test
    @DisplayName("Test Movie Deletion with Mock")
    void testDeleteMovieWithMock() {
        // Setup
        String movieTitle = "Movie to Delete";
        
        // Test
        assertDoesNotThrow(() -> movieService.deleteMovie(movieTitle));
        
        // Verify
        verify(mockConnector, times(1)).executeWrite(anyString(), any());
    }

    @Test
    @DisplayName("Test Add Actor to Movie with Mock")
    void testAddActorToMovieWithMock() {
        // Setup
        String movieTitle = "Test Movie";
        String actorName = "Test Actor";
        
        // Test
        assertDoesNotThrow(() -> movieService.addActor(movieTitle, actorName));
        
        // Verify
        verify(mockConnector, times(1)).executeWrite(anyString(), any());
    }

    @Test
    @DisplayName("Test Add Director to Movie with Mock")
    void testAddDirectorToMovieWithMock() {
        // Setup
        String movieTitle = "Test Movie";
        String directorName = "Test Director";
        
        // Test
        assertDoesNotThrow(() -> movieService.addDirector(movieTitle, directorName));
        
        // Verify
        verify(mockConnector, times(1)).executeWrite(anyString(), any());
    }

    @Test
    @DisplayName("Test Service Constructor Validation")
    void testServiceConstructorValidation() {
        // Test that service constructor accepts null connector gracefully
        // The constructor doesn't validate null, so it should not throw exception
        assertDoesNotThrow(() -> {
            Neo4jMovieService serviceWithNullConnector = new Neo4jMovieService(null);
            assertNotNull(serviceWithNullConnector);
        });
        
        // Test that service constructor works with valid connector
        assertDoesNotThrow(() -> {
            Neo4jMovieService serviceWithValidConnector = new Neo4jMovieService(mockConnector);
            assertNotNull(serviceWithValidConnector);
        });
    }

    @Test
    @DisplayName("Test Connector Exception Handling")
    void testConnectorExceptionHandling() {
        // Setup - make connector throw exception
        doThrow(new RuntimeException("Database connection failed"))
            .when(mockConnector).executeWrite(anyString(), any());
        
        // Test that service handles exceptions gracefully
        Movie movie = new Movie("Test Movie", 2023, "Test", "Test");
        
        assertThrows(RuntimeException.class, () -> {
            movieService.createMovie(movie);
        });
    }

    @Test
    @DisplayName("Test Multiple Operations Verification")
    void testMultipleOperationsVerification() {
        // Setup
        Movie movie = new Movie("Multi Test Movie", 2023, "Test", "Test");
        String actorName = "Test Actor";
        String directorName = "Test Director";
        
        // Test multiple operations
        assertDoesNotThrow(() -> {
            movieService.createMovie(movie);
            movieService.addActor(movie.getTitle(), actorName);
            movieService.addDirector(movie.getTitle(), directorName);
        });
        
        // Verify each operation was called once
        verify(mockConnector, times(3)).executeWrite(anyString(), any());
    }

    @Test
    @DisplayName("Test Movie Object Creation")
    void testMovieObjectCreation() {
        // Test that Movie objects can be created properly
        Movie movie = new Movie("Test Movie", 2023, "Test Genre", "Test Description");
        
        assertNotNull(movie);
        assertEquals("Test Movie", movie.getTitle());
        assertEquals(2023, movie.getYear());
        assertEquals("Test Genre", movie.getGenre());
        assertEquals("Test Description", movie.getDescription());
    }

    @Test
    @DisplayName("Test Service Method Execution Order")
    void testServiceMethodExecutionOrder() {
        // Setup
        Movie movie = new Movie("Order Test Movie", 2023, "Test", "Test");
        
        // Test methods in sequence
        assertDoesNotThrow(() -> movieService.createMovie(movie));
        assertDoesNotThrow(() -> movieService.addActor(movie.getTitle(), "Actor"));
        assertDoesNotThrow(() -> movieService.addDirector(movie.getTitle(), "Director"));
        assertDoesNotThrow(() -> movieService.updateMovie(movie.getTitle(), movie));
        
        // Verify all methods were called
        verify(mockConnector, times(4)).executeWrite(anyString(), any());
    }
}