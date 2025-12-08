package org.daodao.jdbc.service;

import lombok.extern.slf4j.Slf4j;
import org.daodao.jdbc.connectors.Neo4jConnector;
import org.daodao.jdbc.model.Movie;
import org.daodao.jdbc.model.Person;
import org.neo4j.driver.Result;
import org.neo4j.driver.Record;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class Neo4jMovieService {
    private final Neo4jConnector connector;

    public Neo4jMovieService(Neo4jConnector connector) {
        this.connector = connector;
    }

    public void createMovie(Movie movie) {
        String query = "MERGE (m:Movie {title: $title}) SET m.year = $year, m.genre = $genre, m.description = $description";
        Map<String, Object> params = new HashMap<>();
        params.put("title", movie.getTitle());
        params.put("year", movie.getYear());
        params.put("genre", movie.getGenre());
        params.put("description", movie.getDescription());
        connector.executeWrite(query, params);
        log.info("Movie created: {}", movie.getTitle());
    }

    public Movie getMovie(String title) {
        String query = "MATCH (m:Movie {title: $title}) RETURN m.year as year, m.genre as genre, m.description as description";
        Map<String, Object> params = new HashMap<>();
        params.put("title", title);
        
        try (var session = connector.getSession()) {
            return session.readTransaction(tx -> {
                Result result = tx.run(query, params);
                if (result.hasNext()) {
                    Record record = result.single();
                    Movie movie = new Movie();
                    movie.setTitle(title);
                    var yearValue = record.get("year");
                    movie.setYear(yearValue.isNull() ? null : yearValue.asInt());
                    var genreValue = record.get("genre");
                    movie.setGenre(genreValue.isNull() ? null : genreValue.asString());
                    var descriptionValue = record.get("description");
                    movie.setDescription(descriptionValue.isNull() ? null : descriptionValue.asString());
                    return movie;
                }
                return null;
            });
        }
    }

    public List<Movie> getAllMovies() {
        String query = "MATCH (m:Movie) RETURN m.title as title, m.year as year, m.genre as genre, m.description as description ORDER BY m.title";
        Map<String, Object> params = new HashMap<>();
        
        try (var session = connector.getSession()) {
            return session.readTransaction(tx -> {
                Result result = tx.run(query, params);
                List<Movie> movies = new ArrayList<>();
                while (result.hasNext()) {
                    Record record = result.next();
                    Movie movie = new Movie();
                    movie.setTitle(record.get("title").asString());
                    var yearValue = record.get("year");
                    movie.setYear(yearValue.isNull() ? null : yearValue.asInt());
                    var genreValue = record.get("genre");
                    movie.setGenre(genreValue.isNull() ? null : genreValue.asString());
                    var descriptionValue = record.get("description");
                    movie.setDescription(descriptionValue.isNull() ? null : descriptionValue.asString());
                    movies.add(movie);
                }
                return movies;
            });
        }
    }

    public void updateMovie(String title, Movie updatedMovie) {
        String query = "MATCH (m:Movie {title: $title}) SET m.year = $year, m.genre = $genre, m.description = $description";
        Map<String, Object> params = new HashMap<>();
        params.put("title", title);
        params.put("year", updatedMovie.getYear());
        params.put("genre", updatedMovie.getGenre());
        params.put("description", updatedMovie.getDescription());
        connector.executeWrite(query, params);
        log.info("Movie updated: {}", title);
    }

    public void deleteMovie(String title) {
        String query = "MATCH (m:Movie {title: $title}) DETACH DELETE m";
        Map<String, Object> params = new HashMap<>();
        params.put("title", title);
        connector.executeWrite(query, params);
        log.info("Movie deleted: {}", title);
    }

    public void addActor(String movieTitle, String actorName) {
        String query = "MATCH (m:Movie {title: $movieTitle}), (a:Person {name: $actorName}) MERGE (a)-[:ACTED_IN]->(m)";
        Map<String, Object> params = new HashMap<>();
        params.put("movieTitle", movieTitle);
        params.put("actorName", actorName);
        connector.executeWrite(query, params);
        log.info("Actor {} added to movie {}", actorName, movieTitle);
    }

    public void addDirector(String movieTitle, String directorName) {
        String query = "MATCH (m:Movie {title: $movieTitle}), (d:Person {name: $directorName}) MERGE (d)-[:DIRECTED]->(m)";
        Map<String, Object> params = new HashMap<>();
        params.put("movieTitle", movieTitle);
        params.put("directorName", directorName);
        connector.executeWrite(query, params);
        log.info("Director {} added to movie {}", directorName, movieTitle);
    }

    public List<Person> getActorsInMovie(String movieTitle) {
        String query = "MATCH (a:Person)-[:ACTED_IN]->(m:Movie {title: $movieTitle}) RETURN a.name as name, a.birthYear as birthYear, a.nationality as nationality";
        Map<String, Object> params = new HashMap<>();
        params.put("movieTitle", movieTitle);
        
        try (var session = connector.getSession()) {
            return session.readTransaction(tx -> {
                Result result = tx.run(query, params);
                List<Person> actors = new ArrayList<>();
                while (result.hasNext()) {
                    Record record = result.next();
                    Person person = new Person();
                    person.setName(record.get("name").asString());
                    var birthYearValue = record.get("birthYear");
                    person.setBirthYear(birthYearValue.isNull() ? null : birthYearValue.asInt());
                    var nationalityValue = record.get("nationality");
                    person.setNationality(nationalityValue.isNull() ? null : nationalityValue.asString());
                    actors.add(person);
                }
                return actors;
            });
        }
    }

    public List<Person> getDirectorsOfMovie(String movieTitle) {
        String query = "MATCH (d:Person)-[:DIRECTED]->(m:Movie {title: $movieTitle}) RETURN d.name as name, d.birthYear as birthYear, d.nationality as nationality";
        Map<String, Object> params = new HashMap<>();
        params.put("movieTitle", movieTitle);
        
        try (var session = connector.getSession()) {
            return session.readTransaction(tx -> {
                Result result = tx.run(query, params);
                List<Person> directors = new ArrayList<>();
                while (result.hasNext()) {
                    Record record = result.next();
                    Person person = new Person();
                    person.setName(record.get("name").asString());
                    var birthYearValue = record.get("birthYear");
                    person.setBirthYear(birthYearValue.isNull() ? null : birthYearValue.asInt());
                    var nationalityValue = record.get("nationality");
                    person.setNationality(nationalityValue.isNull() ? null : nationalityValue.asString());
                    directors.add(person);
                }
                return directors;
            });
        }
    }

    public List<Movie> getMoviesByActor(String actorName) {
        String query = "MATCH (a:Person {name: $actorName})-[:ACTED_IN]->(m:Movie) RETURN m.title as title, m.year as year, m.genre as genre, m.description as description";
        Map<String, Object> params = new HashMap<>();
        params.put("actorName", actorName);
        
        try (var session = connector.getSession()) {
            return session.readTransaction(tx -> {
                Result result = tx.run(query, params);
                List<Movie> movies = new ArrayList<>();
                while (result.hasNext()) {
                    Record record = result.next();
                    Movie movie = new Movie();
                    movie.setTitle(record.get("title").asString());
                    var yearValue = record.get("year");
                    movie.setYear(yearValue.isNull() ? null : yearValue.asInt());
                    var genreValue = record.get("genre");
                    movie.setGenre(genreValue.isNull() ? null : genreValue.asString());
                    var descriptionValue = record.get("description");
                    movie.setDescription(descriptionValue.isNull() ? null : descriptionValue.asString());
                    movies.add(movie);
                }
                return movies;
            });
        }
    }

    public List<Movie> getMoviesByDirector(String directorName) {
        String query = "MATCH (d:Person {name: $directorName})-[:DIRECTED]->(m:Movie) RETURN m.title as title, m.year as year, m.genre as genre, m.description as description";
        Map<String, Object> params = new HashMap<>();
        params.put("directorName", directorName);
        
        try (var session = connector.getSession()) {
            return session.readTransaction(tx -> {
                Result result = tx.run(query, params);
                List<Movie> movies = new ArrayList<>();
                while (result.hasNext()) {
                    Record record = result.next();
                    Movie movie = new Movie();
                    movie.setTitle(record.get("title").asString());
                    var yearValue = record.get("year");
                    movie.setYear(yearValue.isNull() ? null : yearValue.asInt());
                    var genreValue = record.get("genre");
                    movie.setGenre(genreValue.isNull() ? null : genreValue.asString());
                    var descriptionValue = record.get("description");
                    movie.setDescription(descriptionValue.isNull() ? null : descriptionValue.asString());
                    movies.add(movie);
                }
                return movies;
            });
        }
    }
}