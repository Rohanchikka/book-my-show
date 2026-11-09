-- Runs once, the first time the Postgres volume is created.
-- `booking` is the service's database; `booking_test` is for the test suite.
-- (Later: one database per service, e.g. catalog, payment. Never a shared schema.)
CREATE DATABASE booking_test;
