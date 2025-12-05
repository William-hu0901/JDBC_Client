package org.daodao.jdbc.connectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.daodao.jdbc.config.Neo4jConfig;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.Result;
import org.neo4j.driver.Value;

import java.util.HashMap;
import java.util.Map;

public class Neo4jConnector {
    private static final Logger log = LoggerFactory.getLogger(Neo4jConnector.class);
    private final Driver driver;
    private final Neo4jConfig config;

    public Neo4jConnector(Neo4jConfig config) {
        this.config = config;
        this.driver = GraphDatabase.driver(config.getUri(), 
            AuthTokens.basic(config.getUsername(), config.getPassword()));
        log.info("Connected to Neo4j at {}", config.getUri());
    }

    public Session getSession() {
        return driver.session();
    }

    public void close() {
        if (driver != null) {
            driver.close();
            log.info("Neo4j connection closed");
        }
    }

    public void executeWrite(String query, java.util.Map<String, Object> parameters) {
        try (Session session = getSession()) {
            session.writeTransaction(tx -> {
                Result result = tx.run(query, parameters);
                log.info("Write query executed: {}", query);
                return result;
            });
        }
    }

    public Result executeRead(String query, java.util.Map<String, Object> parameters) {
        try (Session session = getSession()) {
            return session.readTransaction(tx -> {
                Result result = tx.run(query, parameters);
                log.info("Read query executed: {}", query);
                return result;
            });
        }
    }

    public boolean isDatabaseEmpty() {
        String query = "MATCH (n) RETURN count(n) as count";
        Map<String, Object> params = new HashMap<>();
        try (Session session = getSession()) {
            return session.readTransaction(tx -> {
                Result result = tx.run(query, params);
                int count = result.single().get("count").asInt();
                log.info("Database node count: {}", count);
                return count == 0;
            });
        }
    }
}