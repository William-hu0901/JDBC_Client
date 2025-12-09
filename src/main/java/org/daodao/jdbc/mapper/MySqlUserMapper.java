package org.daodao.jdbc.mapper;

import org.apache.ibatis.annotations.*;
import org.daodao.jdbc.model.User;

import java.util.List;

@Mapper
public interface MySqlUserMapper {
    
    @Insert("INSERT INTO users (username, email, age, city) VALUES (#{username}, #{email}, #{age}, #{city})")
    @Options(useGeneratedKeys = true, keyProperty = "id")
    int insertUser(User user);
    
    @Update("UPDATE users SET email = #{email}, age = #{age}, city = #{city}, updated_at = CURRENT_TIMESTAMP WHERE username = #{username}")
    int updateUser(User user);
    
    @Delete("DELETE FROM users WHERE username = #{username}")
    int deleteUserByUsername(@Param("username") String username);
    
    @Select("SELECT * FROM users WHERE username = #{username}")
    User findUserByUsername(@Param("username") String username);
    
    @Select("SELECT * FROM users WHERE city = #{city} ORDER BY username")
    List<User> findUsersByCity(@Param("city") String city);
    
    @Select("SELECT * FROM users ORDER BY username LIMIT #{limit}")
    List<User> findAllUsers(@Param("limit") int limit);
    
    @Select("SELECT COUNT(*) FROM users")
    int getUserCount();
    
    @Select("SELECT * FROM users WHERE age BETWEEN #{minAge} AND #{maxAge} ORDER BY age")
    List<User> findUsersByAgeRange(@Param("minAge") int minAge, @Param("maxAge") int maxAge);
    
    @Insert("INSERT IGNORE INTO users (username, email, age, city) VALUES (#{username}, #{email}, #{age}, #{city})")
    int insertUserIgnore(User user);
}