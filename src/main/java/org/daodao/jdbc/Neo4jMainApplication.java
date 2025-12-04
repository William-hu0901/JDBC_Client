package org.daodao.jdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.daodao.jdbc.config.Neo4jConfig;
import org.daodao.jdbc.connectors.Neo4jConnector;
import org.daodao.jdbc.model.Movie;
import org.daodao.jdbc.model.Person;
import org.daodao.jdbc.service.Neo4jDatabaseInitializer;
import org.daodao.jdbc.service.Neo4jMovieService;

public class Neo4jMainApplication {
    private static final Logger log = LoggerFactory.getLogger(Neo4jMainApplication.class);
    public static void main(String[] args) {
        try {
            Neo4jConfig config = new Neo4jConfig();
            Neo4jConnector connector = new Neo4jConnector(config);
            Neo4jDatabaseInitializer initializer = new Neo4jDatabaseInitializer(connector);
            Neo4jMovieService movieService = new Neo4jMovieService(connector);

            initializer.initializeDatabase();

            demonstrateCRUDOperations(movieService);

            connector.close();
        } catch (Exception e) {
            log.error("Error in Neo4j application", e);
        }
    }

    private static void demonstrateCRUDOperations(Neo4jMovieService movieService) {
        log.info("=== Demonstrating CRUD Operations ===");

        // Use unique movie name with timestamp to avoid conflicts
        String timestamp = String.valueOf(System.currentTimeMillis());
        String movieTitle = "TestMovie_" + timestamp;
        Movie newMovie = new Movie(movieTitle, 2014, "Science Fiction", "A team of explorers travel through a wormhole in space");
        movieService.createMovie(newMovie);

        Movie retrievedMovie = movieService.getMovie(movieTitle);
        if (retrievedMovie != null) {
            log.info("Retrieved movie: {} ({})", retrievedMovie.getTitle(), retrievedMovie.getYear());
        }

        newMovie.setGenre("Adventure");
        movieService.updateMovie(movieTitle, newMovie);

        Movie updatedMovie = movieService.getMovie(movieTitle);
        if (updatedMovie != null) {
            log.info("Updated movie genre: {}", updatedMovie.getGenre());
        }

        log.info("All movies in database:");
        for (Movie movie : movieService.getAllMovies()) {
            log.info("- {} ({}) - {}", movie.getTitle(), movie.getYear(), movie.getGenre());
        }

        log.info("Actors in The Matrix:");
        for (Person actor : movieService.getActorsInMovie("The Matrix")) {
            log.info("- {} (Born: {})", actor.getName(), actor.getBirthYear());
        }

        log.info("Directors of Inception:");
        for (Person director : movieService.getDirectorsOfMovie("Inception")) {
            log.info("- {} ({})", director.getName(), director.getNationality());
        }

        log.info("Movies by Leonardo DiCaprio:");
        for (Movie movie : movieService.getMoviesByActor("Leonardo DiCaprio")) {
            log.info("- {} ({})", movie.getTitle(), movie.getYear());
        }

        log.info("Movies directed by Christopher Nolan:");
        for (Movie movie : movieService.getMoviesByDirector("Christopher Nolan")) {
            log.info("- {} ({})", movie.getTitle(), movie.getYear());
        }

        // Clean up test movie
        movieService.deleteMovie(movieTitle);
        log.info("Test movie '{}' deleted", movieTitle);
    }
}