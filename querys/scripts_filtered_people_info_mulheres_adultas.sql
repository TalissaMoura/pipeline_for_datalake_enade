-- querys for redshift
-- danger zone
--delete from filtered_people_info_mulheres_adultas;
--delete from ibge_data_cities;
--delete from ibge_data_states;
--drop table filtered_people_info_mulheres_adultas;
--drop table ibge_data_cities;
--drop table ibge_data_states;

-- DDLs

create table filtered_people_info_mulheres_adultas (
    id INT PRIMARY KEY,
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
    horas_trabalhadas NUMERIC(6,3),
    anos_escolaridade NUMERIC(6,3)
);

create table ibge_data_cities(
                nome VARCHAR(150),
                microrregiao VARCHAR(150),
                mesorregiao VARCHAR(150),
                uf VARCHAR(150),
                uf_sigla VARCHAR(2)
);

create table ibge_data_states(
                uf VARCHAR(150),
                uf_sigla VARCHAR(2),
                regiao VARCHAR(150),
                regiao_sigla VARCHAR(2)
);

-- selects 
-- SELECT count(*) FROM filtered_people_info_mulheres_adultas;
-- select count(*) from ibge_data_cities;
-- select count(*) from ibge_data_states;


-- select * from filtered_people_info_mulheres_adultas
-- LIMIT 100;

-- select * from ibge_data_cities;
-- select * from ibge_data_states;

-- select max(idade) from filtered_people_info_mulheres_adultas;

-- Querys 

-- Create a view that adds the 'regiao' info to filtered_people_info_mulheres_adultas
-- create or replace view filter_data_mulheres_adts_regiao AS 
-- select 
--   t1.*,
--   t2.regiao as regiao
-- from filtered_people_info_mulheres_adultas t1 
-- join ibge_data_states t2
-- on t1.uf = t2.uf 

-- --  Qual é a média de renda das pessoas presentes no seu banco de dados?  
-- SELECT AVG(renda) as avg FROM
-- filtered_people_info_mulheres_adultas
-- WHERE renda IS NOT NULL;

-- --  Qual é a média de renda das pessoas que residem no Distrito Federal?  
-- SELECT AVG(renda) as avg FROM (
-- SELECT 
--   renda
-- FROM filtered_people_info_mulheres_adultas
-- WHERE uf='distrito federal'
-- AND renda IS NOT NULL) t1

-- --  Qual é a média de renda das pessoas que moram na região sudeste do país (dos que estão na sua amostra, é claro)?  

-- SELECT AVG(renda) as avg_renda FROM (
-- SELECT 
--   renda
-- FROM filter_data_mulheres_adts_regiao
-- WHERE regiao='sudeste' AND renda is not NULL) t1;

-- --  Qual é o estado brasileiro que possui a menor média de renda?  
-- SELECT 
--   uf,
--   AVG(renda)as renda
-- FROM filter_data_mulheres_adts_regiao 
-- WHERE renda is not NULL
-- GROUP BY uf
-- ORDER BY 2 ASC
-- LIMIT 5;

-- --  Qual é o estado brasileiro que possui a maior média de escolaridade? 
-- SELECT uf,AVG(anos_escolaridade) as avg_escolaridade
-- from filtered_people_info_mulheres_adultas fpima 
-- GROUP BY uf
-- ORDER BY 2 DESC; 

-- --  Qual é a média de escolaridade entre as mulheres que moram no Paraná e estão entre 25 e 30 anos?  
-- SELECT AVG(anos_escolaridade) as avg_escolaridade FROM
-- (SELECT 
--   uf,
--   anos_escolaridade
-- FROM filtered_people_info_mulheres_adultas
-- WHERE uf='parana'
-- AND idade BETWEEN 25 AND 30)t1

-- --  Qual é a média de renda para as pessoas na região Sul do país, que estão na força de trabalho e possuem entre 25 e 35 anos?  
-- SELECT AVG(renda) as avg_renda
-- FROM (
-- SELECT 
--   uf,
--  renda
-- FROM filter_data_mulheres_adts_regiao
-- WHERE regiao='sul' AND 
-- trabalho='pessoas na forca de trabalho'
-- AND idade BETWEEN 25 and 35
-- ) t1
-- --  Qual é a renda média por mesorregião produzida no estado de MG? (Para responder, você deve somar todas as rendas do estado e dividir pelo número de mesorregiões da UF.)  
-- with
--   num_mesorregiao_mg as (
--   select count(*) as num_mesorregiao from (
--     select mesorregiao from 
--     ibge_data_cities t1
--     where t1.uf_sigla ='mg'
--     group by t1.mesorregiao
--     )t2
--  ),
--  sum_renda_mesorregiao as (
--    select SUM(renda) as sum_renda from
--    filtered_people_info_mulheres_adultas
--    where uf='minas gerais'
--   )
-- select sum_renda/num_mesorregiao as result from num_mesorregiao_mg
-- join sum_renda_mesorregiao
-- on sum_renda>num_mesorregiao
 
--  --  Qual é a renda média das mulheres que residem na região Norte do Brasil, possuem graduação, têm idade entre 25 e 35 anos e são pretas ou pardas?  
-- SELECT avg(renda) as avg_renda
-- FROM (
-- SELECT 
--   *
-- FROM filter_data_mulheres_adts_regiao
-- WHERE regiao='norte'
-- AND idade BETWEEN 25 and 35
-- AND cor in ('preta','parda')
-- and graduacao='sim'
-- ) t1