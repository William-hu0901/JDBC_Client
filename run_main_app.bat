@echo off
echo Starting JDBC Client Main Application...

mvn compile exec:java -Dexec.mainClass="org.daodao.jdbc.JdbcClientMain"

echo.
echo Application execution completed.
pause