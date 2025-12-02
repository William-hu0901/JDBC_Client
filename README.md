# JDBC_Client
This is a Client to connect with different databases such as MySQL, PostgreSQL, Oracle, MongoDB, and Neo4j with given parameters.

## PostgreSQL Connection Example
```java
PostgresConnector postgresConnector = new PostgresConnector(
    "database-postgres.chams8ws6974.ap-southeast-1.rds.amazonaws.com",
    5432,
    "postgres",
    "postgres",
    "123"
);
postgresConnector.connect();
```

## MongoDB Connection Example
```java
MongoConnector mongoConnector = new MongoConnector();
mongoConnector.connect();

// Initialize database if empty
if (mongoConnector.isDatabaseEmpty()) {
    mongoConnector.initializeDatabase();
}

// Find all documents
List<Document> documents = mongoConnector.findAllDocuments();

// Find document by email
Document doc = mongoConnector.findDocumentByEmail("user@example.com");

// Update document
boolean updated = mongoConnector.updateDocumentByEmail("user@example.com", "New City", 30);

// Find documents by age range
List<Document> youngUsers = mongoConnector.findDocumentsByAgeRange(20, 30);
```

## Supported Databases
- PostgreSQL
- MySQL
- Oracle
- MongoDB
- Neo4j

## Features
- Simple CRUD operations for each database.
- Custom exception handling for each database.
- Logging with SLF4J and Logback.
- Automatic database initialization for MongoDB.
- Index creation for MongoDB collections.
- Query operations with filters for MongoDB.

## Configuration
The application uses `application.properties` for configuration:

### PostgreSQL Configuration
```properties
postgres.host=your-host
postgres.port=5432
postgres.database=your-database
postgres.username=your-username
postgres.password=your-password
```

### MongoDB Configuration
```properties
mongodb.connection.string=mongodb://localhost:27017
mongodb.database.name=testdb
mongodb.collection.name=users
```

## Project Structure
```
src/main/java/org/daodao/jdbc/
├── JdbcClientMain.java          # Main application entry point
├── config/
│   ├── PostgresConfig.java      # PostgreSQL configuration
│   └── MongoConfig.java         # MongoDB configuration
├── connectors/
│   ├── PostgresConnector.java   # PostgreSQL connection handler
│   └── MongoConnector.java      # MongoDB connection handler
├── exceptions/
│   ├── PostgresException.java   # PostgreSQL exceptions
│   ├── PropertyException.java   # Property loading exceptions
│   └── MongoException.java      # MongoDB exceptions
└── util/
    └── Constants.java            # Application constants

src/test/java/org/daodao/jdbc/
├── connectors/
│   └── PostgresConnectorTest.java   # PostgreSQL integration tests
└── mongodb/
    ├── MongoBasicCRUDTest.java       # Basic CRUD operations
    ├── MongoIndexingAggregationTest.java # Indexing and aggregation
    ├── MongoTransactionTest.java      # Transaction testing
    ├── MongoNewFeaturesTest.java       # Latest MongoDB features
    ├── MongoConnectorMockitoTest.java  # Unit tests with Mockito
    ├── MongoSimpleTest.java          # Basic infrastructure test
    └── TestSuite.java               # Test suite orchestrator
```

## Requirements
- Java 21
- MongoDB running on localhost:27017 (for integration tests)
- Maven for dependency management

## Running the Application
```bash
mvn compile exec:java -Dexec.mainClass="org.daodao.jdbc.JdbcClientMain"
```

## Running Tests
```bash
# Run all tests
mvn test

# Run only MongoDB tests (requires MongoDB running)
mvn test -Dtest=MongoBasicCRUDTest

# Run only Mockito tests (unit tests, no MongoDB required)
mvn test -Dtest=MongoConnectorMockitoTest

# Run test suite
mvn test -Dtest=TestSuite
```

## MongoDB Test Coverage
The project includes comprehensive MongoDB tests covering:

### Basic CRUD Operations
- Single and multiple document insertions
- Document queries with filters
- Updates and replacements
- Document deletion
- Counting and distinct operations

### Advanced Features
- Index creation (single, compound, text, unique)
- Aggregation pipelines (group, sort, limit, project, unwind, lookup)
- Transaction support with rollback and retry
- Change streams monitoring
- Time series collections
- Wildcard indexes
- Advanced array update operators

### Testing Strategy
- **Integration Tests**: Require MongoDB instance, test real database operations
- **Unit Tests**: Use Mockito for testing business logic without database dependency
- **Test Suite**: Orchestrates all MongoDB-related tests

### Test Categories
1. **MongoBasicCRUDTest**: Core MongoDB functionality
2. **MongoIndexingAggregationTest**: Indexing and aggregation operations
3. **MongoTransactionTest**: ACID transaction testing
4. **MongoNewFeaturesTest**: Latest MongoDB production features
5. **MongoConnectorMockitoTest**: Unit testing with mocked dependencies

Note: Some tests may be skipped if MongoDB features are not available in the running version or if MongoDB is not accessible.
