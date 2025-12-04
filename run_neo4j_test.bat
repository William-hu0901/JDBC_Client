@echo off
cd /d "c:/Users/delon/IdeaProjects/JDBC_Client"
java -cp "target/classes;target/test-classes;%USERPROFILE%\.m2\repository\org\neo4j\driver\neo4j-java-driver\4.4.3\neo4j-java-driver-4.4.3.jar;%USERPROFILE%\.m2\repository\org\slf4j\slf4j-api\1.7.32\slf4j-api-1.7.32.jar;%USERPROFILE%\.m2\repository\ch\qos\logback\logback-classic\1.2.6\logback-classic-1.2.6.jar;%USERPROFILE%\.m2\repository\ch\qos\logback\logback-core\1.2.6\logback-core-1.2.6.jar" org.daodao.jdbc.Neo4jMainApplication
pause