-- Runs once when the postgres volume is first created.
CREATE DATABASE taskrelay_test OWNER taskrelay;
\c taskrelay
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
