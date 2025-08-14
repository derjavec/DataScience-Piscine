DROP TABLE IF EXISTS data_2022_oct;
DROP TABLE IF EXISTS data_2022_nov;
DROP TABLE IF EXISTS data_2022_dec;
DROP TABLE IF EXISTS data_2023_jan;

CREATE TABLE data_2022_oct (
    event_time TIMESTAMPTZ NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    product_id BIGINT NOT NULL,
    price NUMERIC(10,2) NOT NULL,
    user_id BIGINT NOT NULL,
    user_session UUID,
    processed BOOLEAN DEFAULT FALSE
);

COPY data_2022_oct(event_time, event_type, product_id, price, user_id, user_session)
FROM '/data/customer/data_2022_oct.csv'
DELIMITER ','
CSV HEADER;

CREATE TABLE data_2022_nov (
    event_time TIMESTAMPTZ NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    product_id BIGINT NOT NULL,
    price NUMERIC(10,2) NOT NULL,
    user_id BIGINT NOT NULL,
    user_session UUID,
    processed BOOLEAN DEFAULT FALSE
);

COPY data_2022_nov(event_time, event_type, product_id, price, user_id, user_session)
FROM '/data/customer/data_2022_nov.csv'
DELIMITER ','
CSV HEADER;

CREATE TABLE data_2022_dec (
    event_time TIMESTAMPTZ NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    product_id BIGINT NOT NULL,
    price NUMERIC(10,2) NOT NULL,
    user_id BIGINT NOT NULL,
    user_session UUID,
    processed BOOLEAN DEFAULT FALSE
);

COPY data_2022_dec(event_time, event_type, product_id, price, user_id, user_session)
FROM '/data/customer/data_2022_dec.csv'
DELIMITER ','
CSV HEADER;

CREATE TABLE data_2023_jan (
    event_time TIMESTAMPTZ NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    product_id BIGINT NOT NULL,
    price NUMERIC(10,2) NOT NULL,
    user_id BIGINT NOT NULL,
    user_session UUID,
    processed BOOLEAN DEFAULT FALSE
);

COPY data_2023_jan(event_time, event_type, product_id, price, user_id, user_session)
FROM '/data/customer/data_2023_jan.csv'
DELIMITER ','
CSV HEADER;


-- docker exec -it postgres_piscine psql -U derjavec -d piscineds -f /scripts/tables.sql