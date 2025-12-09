package org.daodao.jdbc.mapper;

import org.apache.ibatis.annotations.*;
import org.daodao.jdbc.model.User;

import java.util.List;

@Mapper
public interface PostgresUserMapper {
    
    @Insert("INSERT INTO users (username, email, age, city, created_at, updated_at) VALUES (#{username}, #{email}, #{age}, #{city}, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)")
    @Options(useGeneratedKeys = true, keyProperty = "id")
    int insertUser(User user);
    
    @Update("UPDATE users SET email = #{email}, age = #{age}, city = #{city}, updated_at = CURRENT_TIMESTAMP WHERE username = #{username}")
    int updateUser(User user);
    
    @Delete("DELETE FROM users WHERE username = #{username}")
    int deleteUserByUsername(@Param("username") String username);
    
    @Select("SELECT id, username, email, age, city, created_at as createdAt, updated_at as updatedAt FROM users WHERE username = #{username}")
    User findUserByUsername(@Param("username") String username);
    
    @Select("SELECT id, username, email, age, city, created_at as createdAt, updated_at as updatedAt FROM users WHERE city = #{city} ORDER BY username")
    List<User> findUsersByCity(@Param("city") String city);
    
    @Select("SELECT id, username, email, age, city, created_at as createdAt, updated_at as updatedAt FROM users ORDER BY username LIMIT #{limit}")
    List<User> findAllUsers(@Param("limit") int limit);
    
    @Select("SELECT COUNT(*) FROM users")
    int getUserCount();
    
    @Select("SELECT id, username, email, age, city, created_at as createdAt, updated_at as updatedAt FROM users WHERE age BETWEEN #{minAge} AND #{maxAge} ORDER BY age")
    List<User> findUsersByAgeRange(@Param("minAge") int minAge, @Param("maxAge") int maxAge);
    
    @Insert("INSERT INTO users (username, email, age, city, created_at, updated_at) VALUES (#{username}, #{email}, #{age}, #{city}, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) ON CONFLICT (username) DO NOTHING")
    int insertUserIgnore(User user);
}