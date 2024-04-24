CREATE DATABASE stresser
    WITH
    OWNER = postgres
    ENCODING = 'UTF8'
    LC_COLLATE = 'en_US.utf8'
    LC_CTYPE = 'en_US.utf8'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1;

CREATE USER name WITH PASSWORD 'password';
GRANT ALL PRIVILEGES ON DATABASE "stresser" to name;
\c stresser;
GRANT pg_read_all_data TO name;
GRANT pg_write_all_data TO name;
create table table_1(a int, b varchar(2048), c text);
create table table_2(a int, b varchar(8192), c text);

