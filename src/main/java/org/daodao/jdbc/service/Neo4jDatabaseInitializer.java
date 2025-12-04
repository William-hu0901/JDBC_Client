package org.daodao.jdbc.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.daodao.jdbc.connectors.Neo4jConnector;
import org.daodao.jdbc.model.Movie;
import org.daodao.jdbc.model.Person;

import java.util.HashMap;
import java.util.Map;

public class Neo4jDatabaseInitializer {
    private static final Logger log = LoggerFactory.getLogger(Neo4jDatabaseInitializer.class);
    private final Neo4jConnector connector;

    public Neo4jDatabaseInitializer(Neo4jConnector connector) {
        this.connector = connector;
    }

    public void initializeDatabase() {
        if (connector.isDatabaseEmpty()) {
            log.info("Database is empty. Creating schema and inserting initial data...");
            createConstraints();
            createIndexes();
            insertSampleData();
            createViews();
            log.info("Database initialization completed");
        } else {
            log.info("Database already contains data. Skipping initialization.");
        }
    }

    private void createConstraints() {
        String movieConstraint = "CREATE CONSTRAINT movie_title_unique IF NOT EXISTS FOR (m:Movie) REQUIRE m.title IS UNIQUE";
        String personConstraint = "CREATE CONSTRAINT person_name_unique IF NOT EXISTS FOR (p:Person) REQUIRE p.name IS UNIQUE";
        
        connector.executeWrite(movieConstraint, new HashMap<>());
        connector.executeWrite(personConstraint, new HashMap<>());
        
        log.info("Database constraints created");
    }

    private void createIndexes() {
        String movieYearIndex = "CREATE INDEX movie_year_index IF NOT EXISTS FOR (m:Movie) ON (m.year)";
        String movieGenreIndex = "CREATE INDEX movie_genre_index IF NOT EXISTS FOR (m:Movie) ON (m.genre)";
        String personBirthYearIndex = "CREATE INDEX person_birth_year_index IF NOT EXISTS FOR (p:Person) ON (p.birthYear)";
        String personNationalityIndex = "CREATE INDEX person_nationality_index IF NOT EXISTS FOR (p:Person) ON (p.nationality)";
        
        connector.executeWrite(movieYearIndex, new HashMap<>());
        connector.executeWrite(movieGenreIndex, new HashMap<>());
        connector.executeWrite(personBirthYearIndex, new HashMap<>());
        connector.executeWrite(personNationalityIndex, new HashMap<>());
        
        log.info("Database indexes created");
    }

    private void insertSampleData() {
        Movie[] movies = {
            new Movie("The Matrix", 1999, "Science Fiction", "A computer hacker learns about the true nature of reality"),
            new Movie("Inception", 2010, "Science Fiction", "A thief who steals corporate secrets through dream-sharing technology"),
            new Movie("The Shawshank Redemption", 1994, "Drama", "Two imprisoned men bond over years, finding redemption"),
            new Movie("Pulp Fiction", 1994, "Crime", "The lives of two mob hitmen intertwine with multiple storylines"),
            new Movie("The Dark Knight", 2008, "Action", "Batman faces the Joker in a battle for Gotham's soul")
        };

        Person[] actors = {
            new Person("Keanu Reeves", 1964, "Canadian"),
            new Person("Leonardo DiCaprio", 1974, "American"),
            new Person("Tim Robbins", 1958, "American"),
            new Person("Morgan Freeman", 1937, "American"),
            new Person("John Travolta", 1954, "American"),
            new Person("Christian Bale", 1974, "British"),
            new Person("Heath Ledger", 1979, "Australian")
        };

        Person[] directors = {
            new Person("Lana Wachowski", 1965, "American"),
            new Person("Lilly Wachowski", 1967, "American"),
            new Person("Christopher Nolan", 1970, "British"),
            new Person("Frank Darabont", 1959, "American"),
            new Person("Quentin Tarantino", 1963, "American")
        };

        for (Movie movie : movies) {
            String createMovieQuery = "MERGE (m:Movie {title: $title}) SET m.year = $year, m.genre = $genre, m.description = $description";
            Map<String, Object> params = new HashMap<>();
            params.put("title", movie.getTitle());
            params.put("year", movie.getYear());
            params.put("genre", movie.getGenre());
            params.put("description", movie.getDescription());
            connector.executeWrite(createMovieQuery, params);
        }

        for (Person person : actors) {
            String createPersonQuery = "MERGE (p:Person {name: $name}) SET p.birthYear = $birthYear, p.nationality = $nationality, p.role = 'Actor'";
            Map<String, Object> params = new HashMap<>();
            params.put("name", person.getName());
            params.put("birthYear", person.getBirthYear());
            params.put("nationality", person.getNationality());
            connector.executeWrite(createPersonQuery, params);
        }

        for (Person person : directors) {
            String createPersonQuery = "MERGE (p:Person {name: $name}) SET p.birthYear = $birthYear, p.nationality = $nationality, p.role = 'Director'";
            Map<String, Object> params = new HashMap<>();
            params.put("name", person.getName());
            params.put("birthYear", person.getBirthYear());
            params.put("nationality", person.getNationality());
            connector.executeWrite(createPersonQuery, params);
        }

        String[][] actorMovieRelations = {
            {"Keanu Reeves", "The Matrix"},
            {"Leonardo DiCaprio", "Inception"},
            {"Leonardo DiCaprio", "The Shawshank Redemption"},
            {"Morgan Freeman", "The Shawshank Redemption"},
            {"John Travolta", "Pulp Fiction"},
            {"Christian Bale", "The Dark Knight"},
            {"Heath Ledger", "The Dark Knight"}
        };

        String[][] directorMovieRelations = {
            {"Lana Wachowski", "The Matrix"},
            {"Lilly Wachowski", "The Matrix"},
            {"Christopher Nolan", "Inception"},
            {"Christopher Nolan", "The Dark Knight"},
            {"Frank Darabont", "The Shawshank Redemption"},
            {"Quentin Tarantino", "Pulp Fiction"}
        };

        for (String[] relation : actorMovieRelations) {
            String createRelationQuery = "MATCH (a:Person {name: $actorName}), (m:Movie {title: $movieTitle}) MERGE (a)-[:ACTED_IN]->(m)";
            Map<String, Object> params = new HashMap<>();
            params.put("actorName", relation[0]);
            params.put("movieTitle", relation[1]);
            connector.executeWrite(createRelationQuery, params);
        }

        for (String[] relation : directorMovieRelations) {
            String createRelationQuery = "MATCH (d:Person {name: $directorName}), (m:Movie {title: $movieTitle}) MERGE (d)-[:DIRECTED]->(m)";
            Map<String, Object> params = new HashMap<>();
            params.put("directorName", relation[0]);
            params.put("movieTitle", relation[1]);
            connector.executeWrite(createRelationQuery, params);
        }

        log.info("Sample data inserted successfully");
    }

    private void createViews() {
        log.info("Neo4j doesn't support traditional views, but we can create stored procedures or use cypher queries as logical views");
    }
}