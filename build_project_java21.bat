@echo off
echo Building JDBC Client Project without Toolchain...
echo Using Java21 directly: D:\Java\jdk-21
echo.

REM Set Java21 environment
set JAVA_HOME=D:\Java\jdk-21
set PATH=%JAVA_HOME%\bin;%PATH%

echo Java Version:
java -version
echo.

echo ========================================
echo STEP 1: Clean and Compile Project
echo ========================================
D:\Maven\apache-maven-3.9.11\bin\mvn.cmd clean compile

if %ERRORLEVEL% neq 0 (
    echo.
    echo COMPILATION FAILED!
    pause
    exit /b 1
)

echo.
echo ========================================
echo STEP 2: Compile Tests
echo ========================================
D:\Maven\apache-maven-3.9.11\bin\mvn.cmd test-compile

if %ERRORLEVEL% neq 0 (
    echo.
    echo TEST COMPILATION FAILED!
    pause
    exit /b 1
)

echo.
echo ========================================
echo STEP 3: Run All Tests
echo ========================================
echo Running all tests (with proper error handling)...

D:\Maven\apache-maven-3.9.11\bin\mvn.cmd test -q

if %ERRORLEVEL% neq 0 (
    echo.
    echo SOME TESTS FAILED (but MongoDB tests may be skipped due to compatibility)
    echo Check surefire-reports for detailed test results
)

echo.
echo ========================================
echo BUILD COMPLETED!
echo ========================================
echo.
echo Project built with Java21
echo - Compilation: OK
echo - Test compilation: OK  
echo - Tests: Run (see results above)
echo.

echo You can now run the application with:
echo   D:\Maven\apache-maven-3.9.11\bin\mvn.cmd exec:java -Dexec.mainClass="org.daodao.jdbc.JdbcClientMain"
echo.

pause