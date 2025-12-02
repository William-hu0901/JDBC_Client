package org.daodao.jdbc.exceptions;

public class MongoException extends RuntimeException {
    
    public MongoException(String message) {
        super(message);
    }
    
    public MongoException(String message, Throwable cause) {
        super(message, cause);
    }
}