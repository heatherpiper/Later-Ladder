package com.heatherpiper.dao;

import com.heatherpiper.exception.DaoException;
import com.heatherpiper.exception.UniqueConstraintViolationException;
import com.heatherpiper.model.RegisterUserDto;
import com.heatherpiper.model.User;
import com.heatherpiper.service.UserService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.stereotype.Component;

import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@Component
public class JdbcUserDao implements UserDao {

    private static final Logger logger = LoggerFactory.getLogger(JdbcUserDao.class);

    @Autowired
    private UserService userService;

    @Autowired
    private final JdbcTemplate jdbcTemplate;

    public JdbcUserDao(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public User getUserById(int userId) {
        User user = null;
        String sql = "SELECT user_id, username, password_hash, role, last_login, last_active FROM users WHERE user_id = ?";
        try {
            SqlRowSet results = jdbcTemplate.queryForRowSet(sql, userId);
            if (results.next()) {
                user = mapRowToUser(results);
            }
        } catch (CannotGetJdbcConnectionException e) {
            throw new DaoException("Unable to connect to server or database", e);
        }
        return user;
    }

    @Override
    public List<User> getUsers() {
        List<User> users = new ArrayList<>();
        String sql = "SELECT user_id, username, password_hash, role, last_login, last_active FROM users";
        try {
            SqlRowSet results = jdbcTemplate.queryForRowSet(sql);
            while (results.next()) {
                User user = mapRowToUser(results);
                users.add(user);
            }
        } catch (CannotGetJdbcConnectionException e) {
            throw new DaoException("Unable to connect to server or database", e);
        }
        return users;
    }

    @Override
    public User getUserByUsername(String username) {
        if (username == null) throw new IllegalArgumentException("Username cannot be null");
        User user = null;
        String sql = "SELECT user_id, username, password_hash, role, last_login, last_active FROM users WHERE username = ?;";
        try {
            SqlRowSet rowSet = jdbcTemplate.queryForRowSet(sql, username);
            if (rowSet.next()) {
                user = mapRowToUser(rowSet);
            }
        } catch (CannotGetJdbcConnectionException e) {
            throw new DaoException("Unable to connect to server or database", e);
        }
        return user;
    }

    @Override
    public User createUser(RegisterUserDto user) {
        User newUser = null;
        OffsetDateTime initTime = OffsetDateTime.now(ZoneOffset.UTC);
        String insertUserSql = "INSERT INTO users (username, password_hash, role, last_login, last_active) values (?, ?, ?, ?, ?) " +
                "RETURNING user_id";
        String password_hash = new BCryptPasswordEncoder().encode(user.getPassword());
        String ssRole = user.getRole().toUpperCase().startsWith("ROLE_") ? user.getRole().toUpperCase() : "ROLE_" + user.getRole().toUpperCase();
        try {
            int newUserId = jdbcTemplate.queryForObject(insertUserSql, int.class, user.getUsername(), password_hash, ssRole, initTime,
                    initTime);
            newUser = getUserById(newUserId);

            userService.createDefaultUserLadder(newUserId);
        } catch (CannotGetJdbcConnectionException e) {
            throw new DaoException("Unable to connect to server or database", e);
        } catch (DataIntegrityViolationException e) {
            throw new UniqueConstraintViolationException("Username is already taken.", e);
        }
        return newUser;
    }

    @Override
    public boolean userExists(int userId) {
        User user = getUserById(userId);
        return user != null;
    }

    @Override
    public void updateLastLogin(String username) {
        OffsetDateTime nowUtc = OffsetDateTime.now(ZoneOffset.UTC);
        String sql = "UPDATE users SET last_login = ? WHERE username = ?";
        jdbcTemplate.update(sql, nowUtc, username);
    }

    @Override
    public void updateLastActive(String username) {
        logger.info("Updating last active for user: {}", username);
        OffsetDateTime nowUtc = OffsetDateTime.now(ZoneOffset.UTC);
        String sql = "UPDATE users SET last_active = ? WHERE username = ?";
        jdbcTemplate.update(sql, nowUtc, username);
    }

    private User mapRowToUser(SqlRowSet rs) {
        User user = new User();
        user.setId(rs.getInt("user_id"));
        user.setUsername(rs.getString("username"));
        user.setPassword(rs.getString("password_hash"));
        user.setAuthorities(Objects.requireNonNull(rs.getString("role")));
        user.setActivated(true);
        Timestamp lastActiveTs = rs.getTimestamp("last_active");
        Timestamp lastLoginTs = rs.getTimestamp("last_login");

        OffsetDateTime lastActiveOd = lastActiveTs != null ? lastActiveTs.toLocalDateTime().atOffset(ZoneOffset.UTC) : null;
        OffsetDateTime lastLoginOd = lastLoginTs != null ? lastLoginTs.toLocalDateTime().atOffset(ZoneOffset.UTC) : null;


        return user;
    }
}
