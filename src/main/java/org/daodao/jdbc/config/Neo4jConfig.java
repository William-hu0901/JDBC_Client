package org.daodao.jdbc.config;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

@Slf4j
public class Neo4jConfig {
    
    private final String uri;
    private final String username;
    private final String password;
    private final String database;

    public Neo4jConfig() {
        Properties props = new Properties();
        try (InputStream input = Neo4jConfig.class.getClassLoader().getResourceAsStream("application.properties")) {
            if (input == null) {
                throw new RuntimeException("Unable to find application.properties");
            }
            props.load(input);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load application.properties", e);
        }
        
        this.uri = props.getProperty("neo4j.uri");
        this.username = props.getProperty("neo4j.username");
        this.password = props.getProperty("neo4j.password");
        this.database = props.getProperty("neo4j.database");
        
        log.info("Neo4j configuration loaded successfully");
    }

    public Neo4jConfig(String uri, String username, String password, String database) {
        this.uri = uri;
        this.username = username;
        this.password = password;
        this.database = database;
    }

    public String getUri() {
        return uri;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getDatabase() {
        return database;
    }
}