# ETL para extração de dados 


O projeto final do curso envolve construir um data lake com dados educacionais armazenados em um banco MongoDB e na API do IBGE. Em seguida, o dado tratado e filtrado deve ser utilizado
para a construção de um data warehouse para realização de consultas voltadas a problema de negócios.

Para melhor dividir as etapas foi realizada a construção de duas DAGs: `get_ibge_data.py` e `get_pnadc_data.py`. 

# Etapas do ETL

- Construção das tabelas 
Para esse ETL estão utilizando dois banco de dados: RDS postgres e redshift. Dessa forma, antes de iniciar as dags é preciso criar as tabelas para receber os dados nesses dois banco de dados. 
 - Tabelas `ibge_data` e `people_info`: Estão nesse [script]() e são as tabelas utilizadas para salvar os dados tratados na instância RDS. 
 - Tabelas `filtered_people_info_mulheres_adultas`,`ibge_data_cities`,`ibge_data_states`: Estão nesse [script]() e são as tabelas para salvar os dados tratados e filtrados no redshift.

- *get_ibge_data.py* 

1. `request_data`e `get_ibge_data`: Primeiro é feito o request da API do IBGE no endpoint `/localidades/distritos`, em seguida, `get_data` transforma a response em json para csv.
2. `upload_to_s3`: Com os dados brutos em csv, eles são transferidos para o bucket `ibgebucket-data-pnadc` na pasta `ibge/raw`. Aqui é a _camada bronze_ dos dados onde temos os dados sem tratamento ainda. 
3. `clean_data`: Nessa parte, começamos o tratamento dos dados. Por se tratar de strings, deixamos todos os dados em mínusculo, removemos acentos e retiramos espaços no inicio e final das palavras, dados com valores `none` são mudados para ''. Ao final, criamos um novo csv com os dados tratados.
4. `upload_clean_data_to_s3` e `transfer_s3_to_rds`: O dado tratado, então, vai para dois locais: 
  - S3: no bucket `ibgebucket-data-pnadc` na pasta `ibge/processed`. Aqui temos a _camada silver_ dos dados. Eles estão tratados e podem ser utilizados para futuras consultas.
  - Instância rds postgres: Para facilitar futuras consultas e filtragem nos dados, do S3 eles são transferidos para um banco de dados na tabela `ibge_data`.
5. `transfer_rds_cities_data_to_s3` e `transfer_rds_states_data_to_s3`: Nessa parte começa a construção da _camada ouro_ onde vamos ter os dados filtrados. Os dados de localização são divididos em cidades e estados. Os dados das cidades contém: `nome` da cidade, `uf` `sigla_uf`,`messoregiao` e `microrregiao`. Já os dados de estados contém: `uf`,`sigla_uf`, `regiao` e `sigla_regiao`. No S3, eles estão na pasta `ibge/filter` salvados em csv. 
6. `transfer_s3_to_redshift_cities` e `transfer_s3_to_redshift_states`: Com os dados tratados e filtrados, podemos transferir essas informações para o data warehouse no redshift. Os dados de cidades são transferidos para a tabela `ibge_data_cities` e os de estados para a tabela `ibge_data_states`

- *get_pnadc_data.py*

1. `get_mongodb_data`: Aqui realizamos a conexão com o MongoDB e recolhemos os dados armazenados.
2. `upload_raw_data_to_s3`: Em seguida, os dados recolhidos pelo mongo são transferidos para o S3 no bucket `ibgebucket-data-pnadc` na pasta `pnadc_data_2023/raw`. (_camada bronze_)
3. `get_s3_raw_data` e grupo `cleans`: Os dados da camada raw são baixados e tratados. No grupo cleans existem duas tasks: 
   - `clean_data_rename_cols`: O nome das colunas `ocup`, `trab`, `horastrab`e `anosesco` são mudados para `ocupacao`, `trabalho`, `horas_trabalhadas` e `anos_escolaridade`. O tipo de dado de cada coluna também é definido:
     - strings: uf,sexo,cor,graduacao,trabalho e ocupacao
     - int:  ano,trimestre e idade
     - float: anos_escolaridade, horas_trabalhadas e renda:
   -  `clean_data_mod_rows`: Aqui os dados em strings são tratados: todas as letras são postas em minusculo, removemos acentos e retiramos espaços no inicio e final das palavras, dados com valores `none` são mudados para ''. Ao final, criamos um novo csv com os dados tratados.
4. upload_clean_data_to_s3: Em seguida, os dados tratados são transferidos para o S3 na pasta `pnadc_data_2023/processed`. (_camada silver_)
5. transfer_s3_to_rds: Em seguida, os dados tratados são transferido para um banco de dados postgres na tabela `people_info`.
6. transfer_rds_data_to_s3: Em seguida, temos a construção da _camada ouro_ filtrando os dados presentes no rds. Para o data warehouse é necessário apenas recolher dados de mulheres na faixa de idade entre 20 e 40. Com os dados recolhidos já são salvos em csv na pasta `pnadc_data_2023/filter`
7. transfer_s3_to_redshift: Em seguida, os dados já tratados e filtrados são transferidos para a tabela `filtered_people_info_mulheres_adultas` no redshift para realização de consultas. 

# Como rodar esse projeto
1. Para construir as instâncias do airflow basta rodar esse comando:
`docker compose up -d`
2. No airflow, é preciso configurar as conexões:
   - ibge_api: definir a conexão HTTP para acessar a API do IBGE. A URL utilizada é `http://servicodados.ibge.gov.br/api/v1/`
   - aws_conn: Aqui é necessário definir suas credenciais de acesso a AWS (`aws_access_key_id` e `aws_secret_access_key`). Lembre-se que seu user na AWS precisa ter acesso as instâncias utilizadas aqui: S3, Redshift e RDS.
   - postgres_conn: Aqui é necessário definir `host`, `login`, `password` e `schema` do banco de dados postgres.
   - mongodb_conn: Aqui é necessário definir `host`, `login` e `password`. Como estamos utilizando o mongodb server também é necessário acrescentar `{"srv":true}` em `Extra`.
   - redshift_conn: Aqui é necessário definir `host`, `login`, `password` e `schema`.
3. Lembre-se de rodar os scripts para definição das tabelas no redshift e postgres. Eles estão disponíveis na pasta [querys]()
4. Agora basta rodar as dags! A ordem delas aqui não importa já que ambas são adquiridas em fonte de dados diferentes.