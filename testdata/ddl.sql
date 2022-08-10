DROP TABLE IF EXISTS company;
CREATE TABLE company (
  company_cd varchar(5) NOT NULL
  , company_name varchar(256) NOT NULL
  , founded_year integer NOT NULL
  , created_at timestamp with time zone NOT NULL
  , updated_at timestamp with time zone NOT NULL
  , revision integer NOT NULL
  , CONSTRAINT company_PKC PRIMARY KEY (company_cd)
) ;