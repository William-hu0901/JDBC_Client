package org.daodao.jdbc.config;

import lombok.extern.slf4j.Slf4j;
import org.daodao.jdbc.exceptions.PropertyException;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

@Slf4j
public class PostgresConfig {
    private static final String PROPERTIES_FILE = "application.properties";
    
    private final String postgresHost;
    private final int postgresPort;
    private final String postgresDatabase;
    private final String postgresUsername;
    private final String postgresPassword;
    private final String postgresSql;
    
    public PostgresConfig() {
        Properties properties = loadProperties();
        this.postgresHost = getProperty(properties, "postgres.host");
        this.postgresPort = Integer.parseInt(getProperty(properties, "postgres.port"));
        this.postgresDatabase = getProperty(properties, "postgres.database");
        this.postgresUsername = getProperty(properties, "postgres.username");
        this.postgresPassword = getProperty(properties, "postgres.password");
        this.postgresSql = getProperty(properties, "postgres.sql");
        
        log.info("Database configuration loaded successfully");
    }
    
    private Properties loadProperties() {
        Properties properties = new Properties();
        try (InputStream input = getClass().getClassLoader().getResourceAsStream(PROPERTIES_FILE)) {
            if (input == null) {
                throw new PropertyException("Unable to find " + PROPERTIES_FILE);
            }
            properties.load(input);
        } catch (IOException e) {
            throw new PropertyException("Error loading properties file: " + PROPERTIES_FILE, e);
        }
        return properties;
    }
    
    private String getProperty(Properties properties, String key) {
        String value = properties.getProperty(key);
        if (value == null || value.trim().isEmpty()) {
            throw new PropertyException("Required property '" + key + "' is missing or empty in " + PROPERTIES_FILE);
        }
        return value.trim();
    }
    
    public String getPostgresHost() {
        return postgresHost;
    }
    
    public int getPostgresPort() {
        return postgresPort;
    }
    
    public String getPostgresDatabase() {
        return postgresDatabase;
    }
    
    public String getPostgresUsername() {
        return postgresUsername;
    }
    
    public String getPostgresPassword() {
        return postgresPassword;
    }
    
    public String getPostgresSql() {
        return postgresSql;
    }
}