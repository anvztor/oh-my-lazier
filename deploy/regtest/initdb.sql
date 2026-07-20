-- Creates the second worker database. The postgres image auto-creates
-- POSTGRES_DB (laz_worker_1); this adds laz_worker_2 for the secondary DVN worker.
CREATE DATABASE laz_worker_2;
GRANT ALL PRIVILEGES ON DATABASE laz_worker_2 TO laz_worker;
