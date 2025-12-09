@echo off
echo Starting All JDBC Client Tests...

echo.
echo === Compiling Project ===
mvn clean compile

echo.
echo === Running Concurrent Test Suite ===
mvn test -Dtest=org.daodao.jdbc.concurrent.TestSuite

echo.
echo === Running Individual Test Classes ===
mvn test -Dtest=org.daodao.jdbc.concurrent.BasicConcurrentTest
mvn test -Dtest=org.daodao.jdbc.concurrent.SimplifiedConcurrentTest
mvn test -Dtest=org.daodao.jdbc.concurrent.ConcurrentDatabaseTest

echo.
echo === Running Database Connector Tests ===
mvn test -Dtest=*ConnectorTest

echo.
echo All tests completed.
pause