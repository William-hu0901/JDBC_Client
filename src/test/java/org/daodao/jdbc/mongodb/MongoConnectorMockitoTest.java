package org.daodao.jdbc.mongodb;

import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.daodao.jdbc.connectors.MongoConnector;
import org.daodao.jdbc.config.MongoConfig;
import org.daodao.jdbc.exceptions.MongoException;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Test class for MongoConnector using Mockito for unit testing.
 * This class tests the business logic of MongoConnector without requiring
 * an actual MongoDB connection.
 */
@ExtendWith(MockitoExtension.class)
class MongoConnectorMockitoTest {
    
    @Mock
    private MongoClient mockMongoClient;
    
    @Mock
    private MongoDatabase mockDatabase;
    
    @Mock
    private MongoCollection<Document> mockCollection;
    
    @Mock
    private MongoIterable<Document> mockMongoIterable;
    
    @Mock
    private MongoCursor<Document> mockCursor;
    
    @Mock
    private FindIterable<Document> mockFindIterable;
    
    @InjectMocks
    private MongoConnector mongoConnector;
    
    private MongoConfig mongoConfig;
    
    @BeforeEach
    void setUp() {
        mongoConfig = new MongoConfig("mongodb://localhost:27017", "testdb", "users");
        
        // Use reflection to set the private fields
        try {
            java.lang.reflect.Field configField = MongoConnector.class.getDeclaredField("config");
            configField.setAccessible(true);
            configField.set(mongoConnector, mongoConfig);
            
            java.lang.reflect.Field clientField = MongoConnector.class.getDeclaredField("mongoClient");
            clientField.setAccessible(true);
            clientField.set(mongoConnector, mockMongoClient);
            
            java.lang.reflect.Field databaseField = MongoConnector.class.getDeclaredField("database");
            databaseField.setAccessible(true);
            databaseField.set(mongoConnector, mockDatabase);
            
            java.lang.reflect.Field collectionField = MongoConnector.class.getDeclaredField("collection");
            collectionField.setAccessible(true);
            collectionField.set(mongoConnector, mockCollection);
            
        } catch (Exception e) {
            fail("Failed to set up test mocks: " + e.getMessage());
        }
    }
    
    @Test
    void testConnectSuccess() {
        // Given
        when(mockMongoClient.getDatabase(anyString())).thenReturn(mockDatabase);
        when(mockDatabase.getCollection(anyString())).thenReturn(mockCollection);
        
        // When & Then
        assertDoesNotThrow(() -> mongoConnector.connect());
        
        verify(mockMongoClient).getDatabase("testdb");
        verify(mockDatabase).getCollection("users");
    }
    
    @Test
    void testConnectFailure() {
        // Given
        when(mockMongoClient.getDatabase(anyString())).thenThrow(new RuntimeException("Connection failed"));
        
        // When & Then
        assertThrows(MongoException.class, () -> mongoConnector.connect());
    }
    
    @Test
    void testDisconnect() {
        // When & Then
        assertDoesNotThrow(() -> mongoConnector.disconnect());
        verify(mockMongoClient).close();
    }
    
    @Test
    void testIsDatabaseEmptyTrue() {
        // Given
        when(mockCollection.countDocuments()).thenReturn(0L);
        
        // When
        boolean isEmpty = mongoConnector.isDatabaseEmpty();
        
        // Then
        assertTrue(isEmpty);
        verify(mockCollection).countDocuments();
    }
    
    @Test
    void testIsDatabaseEmptyFalse() {
        // Given
        when(mockCollection.countDocuments()).thenReturn(5L);
        
        // When
        boolean isEmpty = mongoConnector.isDatabaseEmpty();
        
        // Then
        assertFalse(isEmpty);
        verify(mockCollection).countDocuments();
    }
    
    @Test
    void testInitializeDatabase() {
        // Given
        InsertOneResult mockResult = mock(InsertOneResult.class);
        when(mockCollection.insertMany(any(List.class))).thenReturn(null);
        when(mockCollection.createIndex(any(Bson.class))).thenReturn("test_index");
        
        // When & Then
        assertDoesNotThrow(() -> mongoConnector.initializeDatabase());
        
        verify(mockCollection).insertMany(any(List.class));
        verify(mockCollection, times(2)).createIndex(any(Bson.class));
    }
    
    @Test
    void testFindAllDocuments() {
        // Given
        List<Document> expectedDocuments = Arrays.asList(
                new Document("name", "John").append("age", 30),
                new Document("name", "Jane").append("age", 25)
        );
        
        when(mockCollection.find()).thenReturn(mockFindIterable);
        when(mockFindIterable.into(any(List.class))).thenAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            List<Document> list = invocation.getArgument(0);
            list.addAll(expectedDocuments);
            return list;
        });
        
        // When
        List<Document> result = mongoConnector.findAllDocuments();
        
        // Then
        assertEquals(2, result.size());
        assertEquals("John", result.get(0).getString("name"));
        assertEquals("Jane", result.get(1).getString("name"));
        verify(mockCollection).find();
    }
    
    @Test
    void testFindDocumentByEmailFound() {
        // Given
        String email = "john@example.com";
        Document expectedDocument = new Document("name", "John").append("email", email).append("age", 30);
        
        when(mockCollection.find(any(Bson.class))).thenReturn(mockFindIterable);
        when(mockFindIterable.first()).thenReturn(expectedDocument);
        
        // When
        Document result = mongoConnector.findDocumentByEmail(email);
        
        // Then
        assertNotNull(result);
        assertEquals("John", result.getString("name"));
        assertEquals(email, result.getString("email"));
        verify(mockCollection).find(any(Bson.class));
    }
    
    @Test
    void testFindDocumentByEmailNotFound() {
        // Given
        String email = "nonexistent@example.com";
        
        when(mockCollection.find(any(Bson.class))).thenReturn(mockFindIterable);
        when(mockFindIterable.first()).thenReturn(null);
        
        // When
        Document result = mongoConnector.findDocumentByEmail(email);
        
        // Then
        assertNull(result);
        verify(mockCollection).find(any(Bson.class));
    }
    
    @Test
    void testFindDocumentsByAgeRange() {
        // Given
        List<Document> expectedDocuments = Arrays.asList(
                new Document("name", "John").append("age", 30),
                new Document("name", "Jane").append("age", 25)
        );
        
        when(mockCollection.find(any(Bson.class))).thenReturn(mockFindIterable);
        when(mockFindIterable.into(any(List.class))).thenAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            List<Document> list = invocation.getArgument(0);
            list.addAll(expectedDocuments);
            return list;
        });
        
        // When
        List<Document> result = mongoConnector.findDocumentsByAgeRange(20, 35);
        
        // Then
        assertEquals(2, result.size());
        verify(mockCollection).find(any(Bson.class));
    }
    
    @Test
    void testUpdateDocumentByEmailSuccess() {
        // Given
        String email = "john@example.com";
        String newCity = "Boston";
        int newAge = 31;
        
        UpdateResult mockUpdateResult = mock(UpdateResult.class);
        when(mockUpdateResult.getModifiedCount()).thenReturn(1L);
        when(mockCollection.updateOne(any(Bson.class), any(Bson.class))).thenReturn(mockUpdateResult);
        
        // When
        boolean result = mongoConnector.updateDocumentByEmail(email, newCity, newAge);
        
        // Then
        assertTrue(result);
        verify(mockCollection).updateOne(any(Bson.class), any(Bson.class));
    }
    
    @Test
    void testUpdateDocumentByEmailNotFound() {
        // Given
        String email = "nonexistent@example.com";
        String newCity = "Boston";
        int newAge = 31;
        
        UpdateResult mockUpdateResult = mock(UpdateResult.class);
        when(mockUpdateResult.getModifiedCount()).thenReturn(0L);
        when(mockCollection.updateOne(any(Bson.class), any(Bson.class))).thenReturn(mockUpdateResult);
        
        // When
        boolean result = mongoConnector.updateDocumentByEmail(email, newCity, newAge);
        
        // Then
        assertFalse(result);
        verify(mockCollection).updateOne(any(Bson.class), any(Bson.class));
    }
    
    @Test
    void testDeleteDocumentByEmailSuccess() {
        // Given
        String email = "john@example.com";
        
        DeleteResult mockDeleteResult = mock(DeleteResult.class);
        when(mockDeleteResult.getDeletedCount()).thenReturn(1L);
        when(mockCollection.deleteOne(any(Bson.class))).thenReturn(mockDeleteResult);
        
        // When
        boolean result = mongoConnector.deleteDocumentByEmail(email);
        
        // Then
        assertTrue(result);
        verify(mockCollection).deleteOne(any(Bson.class));
    }
    
    @Test
    void testDeleteDocumentByEmailNotFound() {
        // Given
        String email = "nonexistent@example.com";
        
        DeleteResult mockDeleteResult = mock(DeleteResult.class);
        when(mockDeleteResult.getDeletedCount()).thenReturn(0L);
        when(mockCollection.deleteOne(any(Bson.class))).thenReturn(mockDeleteResult);
        
        // When
        boolean result = mongoConnector.deleteDocumentByEmail(email);
        
        // Then
        assertFalse(result);
        verify(mockCollection).deleteOne(any(Bson.class));
    }
    
    @Test
    void testGetDocumentCount() {
        // Given
        long expectedCount = 42L;
        when(mockCollection.countDocuments()).thenReturn(expectedCount);
        
        // When
        long result = mongoConnector.getDocumentCount();
        
        // Then
        assertEquals(expectedCount, result);
        verify(mockCollection).countDocuments();
    }
    
    @Test
    void testExceptionHandling() {
        // Given
        when(mockCollection.countDocuments()).thenThrow(new RuntimeException("Database error"));
        
        // When & Then
        assertThrows(MongoException.class, () -> mongoConnector.getDocumentCount());
    }
    
    @Test
    void testFindAllDocumentsWithException() {
        // Given
        when(mockCollection.find()).thenThrow(new RuntimeException("Connection error"));
        
        // When & Then
        assertThrows(MongoException.class, () -> mongoConnector.findAllDocuments());
    }
    
    @Test
    void testUpdateDocumentWithException() {
        // Given
        when(mockCollection.updateOne(any(Bson.class), any(Bson.class)))
                .thenThrow(new RuntimeException("Update failed"));
        
        // When & Then
        assertThrows(MongoException.class, 
                () -> mongoConnector.updateDocumentByEmail("test@example.com", "City", 30));
    }
    
    @Test
    void testDeleteDocumentWithException() {
        // Given
        when(mockCollection.deleteOne(any(Bson.class)))
                .thenThrow(new RuntimeException("Delete failed"));
        
        // When & Then
        assertThrows(MongoException.class, 
                () -> mongoConnector.deleteDocumentByEmail("test@example.com"));
    }
    
    @Test
    void testInitializeDatabaseWithException() {
        // Given
        when(mockCollection.insertMany(any(List.class)))
                .thenThrow(new RuntimeException("Insert failed"));
        
        // When & Then
        assertThrows(MongoException.class, () -> mongoConnector.initializeDatabase());
    }
}