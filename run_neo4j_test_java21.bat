@echo off
echo Running Neo4j Test Cases with Java21 Toolchain...
echo.

REM Check if Java21 exists at specified path
if exist "D:\Java\jdk-21\bin\java.exe" (
    echo Setting JAVA_HOME to Java21...
    set JAVA_HOME=D:\Java\jdk-21
    set PATH=%JAVA_HOME%\bin;%PATH%
    
    echo Java Version:
    java -version
) else (
    echo WARNING: Java21 not found at D:\Java\jdk-21
    echo Using default Java installation...
    echo.
    echo Java Version:
    java -version
)

echo.
echo Compiling project...
call mvn clean compile -q

if %ERRORLEVEL% neq 0 (
    echo Compilation failed. Please check your Java and Maven setup.
    pause
    exit /b 1
)

echo.
echo Running Neo4j Test Suite...
call mvn test -Dtest=Neo4jTestSuite,Neo4jBasicFunctionalityTest,Neo4jNewFeaturesTest -Dmaven.test.failure.ignore=true -q

echo.
echo Test execution completed.
echo.

REM Test specific commands information
echo You can also run specific tests:
echo   mvn test -Dtest=Neo4jTestSuite
echo   mvn test -Dtest=Neo4jBasicFunctionalityTest  
echo   mvn test -Dtest=Neo4jNewFeaturesTest
echo   mvn test -Dtest=Neo4jTestRunner
echo.

pause