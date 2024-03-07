-- querys for rds

--- danger zone
--drop table people_info;
--drop table ibge_data;
--delete from people_info;
--delete from ibge_data;



CREATE TABLE people_info (
    id SERIAL PRIMARY KEY,
    ano INT,
    trimestre INT,
    uf VARCHAR(150),
    sexo VARCHAR(150),
    idade INT,
    cor VARCHAR(150),
    graduacao VARCHAR (150),
    trabalho  VARCHAR(150),
    ocupacao  VARCHAR(150),
    renda NUMERIC(12,3),
    horas_trabalhadas numeric(6,3),
    anos_escolaridade NUMERIC(6,3)
);

CREATE TABLE ibge_data (
                nome VARCHAR(150),
                microrregiao VARCHAR(150),
                mesorregiao VARCHAR(150),
                uf VARCHAR(150),
                uf_sigla VARCHAR(2),
                regiao VARCHAR(150),
                regiao_sigla VARCHAR(2)
);




-- SELECTS

-- select count(*) from people_info;
-- select count(*) from ibge_data;


-- Querys

-- select * from people_info
-- limit 100;

-- select * from ibge_data
-- limit 100;



