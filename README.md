# JDBC_Client

This is a Client to connect with different databases such as MySQL, PostgreSQL, MongoDB, and Neo4j with given parameters.

## Project Features

- **Multi-Database Support**: Connect to MySQL, PostgreSQL, MongoDB, and Neo4j
- **CRUD Operations**: Complete Create, Read, Update, Delete functionality
- **Modern Java**: Built with Java21 and Maven toolchains
- **Comprehensive Testing**: Extensive test coverage with integration and unit tests
- **Logging**: SLF4J with Logback for proper logging
- **Configuration**: External configuration via application.properties
- **Concurrent Operations**: Thread-safe connection pooling and concurrent database operations

## Connection Examples

### MySQL Connection Example
```java
MySqlConnector mysqlConnector = new MySqlConnector();
mysqlConnector.connect();

// Initialize database if empty
if (mysqlConnector.isDatabaseEmpty()) {
    mysqlConnector.initializeDatabase();
}

// Find all users
List<String> users = mysqlConnector.findAllUsers();

// Insert new user
boolean inserted = mysqlConnector.insertUser("john_doe", "john@example.com", 30, "New York");

// Update user email
boolean updated = mysqlConnector.updateUserEmail("john_doe", "john.new@example.com");

// Delete user
boolean deleted = mysqlConnector.deleteUser("john_doe");

// Find users by city
List<String> cityUsers = mysqlConnector.findUsersByCity("New York");

// Get user count
int userCount = mysqlConnector.getUserCount();
```

### PostgreSQL Connection Example
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

### MongoDB Connection Example
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

### Neo4j Connection Example
```java
// Configuration
Neo4jConfig config = new Neo4jConfig();
Neo4jConnector connector = new Neo4jConnector(config);

// Initialize database if empty
Neo4jDatabaseInitializer initializer = new Neo4jDatabaseInitializer(connector);
Neo4jMovieService movieService = new Neo4jMovieService(connector);
initializer.initializeDatabase();

// CRUD operations
Movie movie = new Movie("Inception", 2010, "Science Fiction", "A thief enters dreams");
movieService.createMovie(movie);

// Add relationships
movieService.addActor("Inception", "Leonardo DiCaprio");
movieService.addDirector("Inception", "Christopher Nolan");

// Query operations
List<Movie> allMovies = movieService.getAllMovies();
List<Person> actors = movieService.getActorsInMovie("Inception");
List<Movie> actorMovies = movieService.getMoviesByActor("Leonardo DiCaprio");

// Update and delete
Movie updated = new Movie("Inception", 2010, "Thriller", "Updated description");
movieService.updateMovie("Inception", updated);
movieService.deleteMovie("Inception");
```

## Supported Databases
- MySQL
- PostgreSQL
- MongoDB
- Neo4j

## Configuration

The application uses `application.properties` for configuration:

### MySQL Configuration
```properties
mysql.host=localhost
mysql.port=3306
mysql.database=testdb
mysql.username=root
mysql.password=
```

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

### Neo4j Configuration
```properties
neo4j.uri=bolt://localhost:7687
neo4j.username=neo4j
neo4j.password=your_password
neo4j.database=neo4j
```

## Requirements
- Java 21 (configured at D:\Java\jdk-21)
- MongoDB running on localhost:27017 (for integration tests)
- Neo4j running on localhost:7687 (for integration tests)
- Maven for dependency management
- Maven toolchains configuration (included)

## Project Structure

```
src/main/java/org/daodao/jdbc/
├── JdbcClientMain.java          # Main application entry point (includes concurrent operations demo)
├── Neo4jMainApplication.java    # Neo4j application entry point
├── config/
│   ├── MySqlConfig.java         # MySQL configuration
│   ├── PostgresConfig.java      # PostgreSQL configuration
│   ├── MongoConfig.java         # MongoDB configuration
│   └── Neo4jConfig.java         # Neo4j configuration
├── connectors/
│   ├── MySqlConnector.java      # MySQL connection handler
│   ├── PostgresConnector.java   # PostgreSQL connection handler
│   ├── MongoConnector.java      # MongoDB connection handler
│   └── Neo4jConnector.java      # Neo4j connection handler
├── exceptions/
│   ├── MySqlException.java       # MySQL exceptions
│   ├── PostgresException.java   # PostgreSQL exceptions
│   ├── PropertyException.java   # Property loading exceptions
│   └── MongoException.java      # MongoDB exceptions
├── model/
│   ├── User.java               # User data model
│   ├── Movie.java              # Movie data model
│   └── Person.java             # Person data model
├── pool/
│   ├── ConnectionPool.java     # Generic connection pool implementation
│   ├── MySqlConnectionPool.java # MySQL connection pool
│   ├── PostgresConnectionPool.java # PostgreSQL connection pool
│   └── DatabaseThreadPoolManager.java # Thread pool manager for concurrent operations
├── service/
│   ├── ConcurrentDatabaseService.java # Concurrent database operations service
│   ├── Neo4jDatabaseInitializer.java # Neo4j database initialization
│   └── Neo4jMovieService.java   # Neo4j movie CRUD service
├── mapper/
│   ├── MySqlUserMapper.java    # MySQL MyBatis mapper
│   └── PostgresUserMapper.java # PostgreSQL MyBatis mapper
└── util/
    └── Constants.java            # Application constants

src/test/java/org/daodao/jdbc/
├── concurrent/
│   ├── BasicConcurrentTest.java       # Basic concurrent database operations tests
│   ├── SimplifiedConcurrentTest.java  # Simplified concurrent operations tests
│   ├── ConcurrentDatabaseTest.java     # Full concurrent database operations tests
│   └── TestSuite.java                 # Concurrent test suite
└── connectors/
    ├── MongoConnectorTest.java        # MongoDB integration tests
    ├── PostgresConnectorTest.java     # PostgreSQL integration tests
    └── MySqlConnectorTest.java        # MySQL integration tests
```

## Running the Application

### Option 1: Using Maven
```bash
mvn compile exec:java -Dexec.mainClass="org.daodao.jdbc.JdbcClientMain"
mvn compile exec:java -Dexec.mainClass="org.daodao.jdbc.Neo4jMainApplication"
```

### Option 2: Using Batch Scripts

#### Basic Neo4j Script (`run_neo4j_test.bat`)
Double-click to run the Neo4j application with automatic classpath configuration.

#### Java21 Toolchain Script (`run_neo4j_test_java21.bat`)
Runs with Java21 toolchain support:
```batch
@echo off
echo Running Neo4j Test Cases with Java21 Toolchain...
echo Setting JAVA_HOME to Java21...
set JAVA_HOME=D:\Java\jdk-21
set PATH=%JAVA_HOME%\bin;%PATH%

echo Java Version:
java -version

echo Compiling with Java21 Toolchain...
call mvn clean compile -T 1C

echo Running Neo4j Test Suite with Java21...
call mvn test -Dmaven.test.failure.ignore=true -DskipTests=false -B

pause
```

### Option 3: Using Maven Toolchains
```bash
# Verify toolchain configuration
mvn toolchain:toolchain

# Compile with toolchain
mvn -Dtoolchain.skip=false clean compile

# Run tests with toolchain
mvn -Dtoolchain.skip=false test -Dtest=org.daodao.jdbc.concurrent.TestSuite

# Build entire project with toolchain
mvn -Dtoolchain.skip=false clean package
```

## Running Tests

### All Tests
```bash
# Run all tests
mvn test

# Run tests with Java21 toolchain
mvn -Dtoolchain.skip=false test
```

### Concurrent Database Tests
```bash
# Run concurrent test suite
mvn test -Dtest=org.daodao.jdbc.concurrent.TestSuite

# Run individual concurrent test classes
mvn test -Dtest=org.daodao.jdbc.concurrent.BasicConcurrentTest
mvn test -Dtest=org.daodao.jdbc.concurrent.SimplifiedConcurrentTest
mvn test -Dtest=org.daodao.jdbc.concurrent.ConcurrentDatabaseTest
```

### Database-Specific Tests

#### MySQL Tests
```bash
mvn test -Dtest=MySqlConnectorTest
```

#### MongoDB Tests
```bash
mvn test -Dtest=MongoConnectorTest
```

#### PostgreSQL Tests
```bash
mvn test -Dtest=PostgresConnectorTest
```

#### Neo4j Tests
```bash
# Run all Neo4j tests
mvn test -Dtest=Neo4jTestSuite

# Run specific Neo4j test classes
mvn test -Dtest=Neo4jCRUDTest
mvn test -Dtest=Neo4jBasicFunctionalityTest
mvn test -Dtest=Neo4jNewFeaturesTest

# Quick test runner
mvn test -Dtest=Neo4jTestRunner
```

## Java21 Toolchain Support

### Setup

The project includes complete Java21 toolchain support:

1. **toolchains.xml**: Defines Java21 toolchain at `D:\Java\jdk-21`
2. **Maven Configuration**: Updated compiler and surefire plugins for Java21
3. **Batch Scripts**: Convenient scripts for Java21 execution
4. **Parallel Compilation**: `-T 1C` enables parallel compilation

### Benefits

- **Consistent Java Version**: Ensures all builds use Java21
- **Parallel Compilation**: Faster build times with multi-core support
- **Isolated Environment**: Separate from system Java installation
- **Reproducible Builds**: Same toolchain across different environments

### Usage

```bash
# Using batch script
run_neo4j_test_java21.bat

# Using Maven with toolchain
mvn -Dtoolchain.skip=false clean compile test

# Build with toolchain
mvn -Dtoolchain.skip=false clean package
```

## Features

### Database Features

#### MySQL Features
- Connection testing and validation
- Basic CRUD operations with prepared statements
- Error handling and connection management
- Support for integration testing with proper setup

#### MongoDB Features
- Connection testing and validation
- Document CRUD operations
- Database initialization when empty
- Query operations with email and age filters
- Graceful handling of unavailable MongoDB instances

#### PostgreSQL Features
- Table creation with various data types
- Data insertion with conflict handling
- Complex SELECT queries with conditions
- UPDATE operations with filters
- DELETE operations with constraints
- Index management (B-Tree, unique, composite, partial)
- Transaction control (commit, rollback, savepoints)
- JSONB data type operations
- Generated columns (PostgreSQL 12+)
- UPSERT operations (INSERT ... ON CONFLICT)
- Array operations and functions
- Full-text search capabilities
- Window functions
- Table partitioning (PostgreSQL 10+)

#### Neo4j Features
- Node creation and retrieval operations
- Relationship management (ACTED_IN, DIRECTED)
- Graph traversal and path finding
- Data updates and deletions with transaction handling
- Complex Cypher queries with filtering and aggregation
- Index and constraint management
- Graph database initialization with sample data
- Advanced Cypher features (subqueries, pattern comprehensions)
- Multi-database operations and switching

### Application Features
- Simple CRUD operations for each database
- Custom exception handling for each database
- Logging with SLF4J and Logback
- Automatic database initialization for MySQL and MongoDB
- Schema, table, and index creation for MySQL
- View creation for MySQL
- Index creation for MongoDB collections
- Query operations with filters for MongoDB
- Java21 compatibility with Maven toolchains
- Comprehensive test coverage
- Parallel compilation support
- Concurrent database operations with thread-safe connection pooling

## Troubleshooting

### Common Issues

1. **Java21 not found at D:\Java\jdk-21**
   - Install Java21 at the specified location
   - Update toolchains.xml with correct path
   - Scripts will fall back to system Java if needed

2. **Neo4j connection failed**
   - Ensure Neo4j is running on localhost:7687
   - Verify credentials in application.properties
   - Check database name configuration

3. **MongoDB connection failed**
   - Ensure MongoDB is running on localhost:27017
   - Check connection string in application.properties

4. **Test failures**
   - Verify databases are running
   - Check connection parameters
   - Ensure proper configuration in application.properties

5. **Maven build issues**
   - Ensure Java21 is properly installed
   - Check Maven version compatibility (3.6+)
   - Verify toolchain configuration

### Verification Commands

```bash
# Check Java version
java -version

# Check Maven version
mvn -version

# Verify toolchain
mvn toolchain:toolchain

# Test compilation
mvn compile -Dtoolchain.skip=false

# Run specific test
mvn test -Dtest=org.daodao.jdbc.concurrent.TestSuite -Dtoolchain.skip=false
```

## License

This project is for educational and demonstration purposes.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Ensure all tests pass with Java21 toolchain
6. Submit a pull request