-- Differential-test database bootstrap (runs once at container init).
-- Creates the role and the empty databases; SCHEMA comes later, owned
-- by code, not here: Java Flyway migrates on boot, and the harness
-- applies the embedded goose set to Go databases at test startup when
-- tables are absent (ensureSchema). Init scripts cannot run migrations
-- (no goose binary in the stock image, and baking one in would fork
-- the migration source) — so this file stops at roles + empty DBs,
-- and anything depending on schema must go through the harness path.
CREATE USER cmp WITH PASSWORD 'x' SUPERUSER;
CREATE DATABASE equalix_java OWNER cmp;
CREATE DATABASE equalix_go OWNER cmp;
CREATE DATABASE equalix_go2 OWNER cmp;
