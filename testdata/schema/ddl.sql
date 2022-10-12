DROP TABLE IF EXISTS company
;
CREATE TABLE company(
    company_cd varchar(5) NOT NULL,
    company_name varchar(256) NOT NULL,
    founded_year integer NOT NULL,
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL,
    revision integer NOT NULL,
    CONSTRAINT company_pkc PRIMARY KEY(company_cd)
)
;

DROP TABLE IF EXISTS test_x
;
CREATE TABLE test_x(
    id varchar NOT NULL,
    a boolean NOT NULL,
    b bytea NOT NULL,
    c char NOT NULL,
    d date NOT NULL,
    e real NOT NULL,
    f double precision NOT NULL,
    g json NOT NULL,
    h jsonb NOT NULL,
    i inet NOT NULL,
    j smallint NOT NULL,
    k integer NOT NULL,
    l bigint NOT NULL,
    m interval NOT NULL,
    n numeric NOT NULL,
    o oid NOT NULL,
    p text NOT NULL,
    q time NOT NULL,
    s timestamp NOT NULL,
    t timestamp with time zone NOT NULL,
    u uuid NOT NULL,
    v character varying NOT NULL,
    w smallserial NOT NULL,
    x serial NOT NULL,
    y bigserial NOT NULL,
    z bit NOT NULL,
    CONSTRAINT test_x_pkc PRIMARY KEY(id)
)
;

DROP TABLE IF EXISTS temperature
;
CREATE TABLE temperature(
    ymd varchar(8) NOT NULL,
    value numeric(4,1) NOT NULL,
    CONSTRAINT temperature_pkc PRIMARY KEY(ymd)
) PARTITION BY RANGE (ymd)
;
DROP TABLE IF EXISTS temperature_2021_2022
;
CREATE TABLE temperature_2021_2022 PARTITION OF temperature FOR VALUES FROM ('20210101') TO ('20220101')
;
