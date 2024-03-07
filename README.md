# Data Lake with education and geolocation data
The final project of the course involves building a data lake with educational data stored in a MongoDB database and from the IBGE (Brazilian Institute of Geography and Statistics) API. Then, the processed and filtered data should be used to build a data warehouse for querying business problems.

To better divide the stages, two DAGs were created: `get_ibge_data.py` and `get_pnadc_data.py`.

# Steps

- Table Construction
For the ETL stages, two databases are being used: RDS Postgres and Redshift. Therefore, before starting the DAGs, it is necessary to create the tables to receive the data in these two databases.

    Tables ibge_data and people_info: These are in this [script](https://github.com/TalissaMoura/pipeline_for_datalake_enade/blob/main/querys/scripts_people_info.sql), and they are the tables used to save the treated data in the RDS instance.
    Tables filtered_people_info_mulheres_adultas, ibge_data_cities, ibge_data_states: These are in this [script](https://github.com/TalissaMoura/pipeline_for_datalake_enade/blob/main/querys/scripts_filtered_people_info_mulheres_adultas.sql), and they are the tables to save the treated and filtered data in Redshift.

- *get_ibge_data.py*
 
1. `request_data` and `get_ibge_data`: First, the IBGE API is requested at the endpoint `/localidades/distritos`, then `get_data` transforms the response into JSON to CSV.
2. `upload_to_s3`: With the raw data in CSV, it is transferred to the `ibgebucket-data-pnadc` bucket in the `ibge/raw` folder. Here is the _bronze layer_ of the data where we have the data untreated yet.
3. `clean_data`: In this part, we start treating the data. Since it is strings, we leave all the data in lowercase, remove accents, and remove spaces at the beginning and end of words. Data with `none` values are changed to ''. Finally, we create a new CSV with the treated data.
4. `upload_clean_data_to_s3` and `transfer_s3_to_rds`: The treated data then goes to two locations:
   - S3: in the `ibgebucket-data-pnadc` bucket in the `ibge/processed` folder. Here we have the _silver layer_ of the data. They are treated and can be used for future queries.
   - RDS Postgres instance: To facilitate future queries and filtering in the data, from S3, they are transferred to a database in the `ibge_data` table.
5. `transfer_rds_cities_data_to_s3` and `transfer_rds_states_data_to_s3`: This is where the construction of the _gold layer_ begins, where we will have the filtered data. Location data is divided into cities and states. City data contains: `city name`, `state abbreviation`, `main region`, and `micro-region`. State data contains: `state`, `state abbreviation`, `region`, and `region abbreviation`. In S3, they are in the `ibge/filter` folder saved as CSV.
6. `transfer_s3_to_redshift_cities` and `transfer_s3_to_redshift_states`: With the treated and filtered data, we can transfer this information to the data warehouse in Redshift. City data is transferred to the `ibge_data_cities` table, and state data is transferred to the `ibge_data_states` table.


- *get_pnadc_data.py*

1. `get_mongodb_data`: Here we connect to MongoDB and gather the stored data.
2. `upload_raw_data_to_s3`: Then, the data collected by mongo is transferred to S3 in the `ibgebucket-data-pnadc` bucket in the `pnadc_data_2023/raw` folder. (_bronze layer_)
3. `get_s3_raw_data` and `cleans` group: The data from the raw layer is downloaded and treated. In the cleans group, there are two tasks:
   - `clean_data_rename_cols`: The column names `occupation`, `work`, `hours_worked`, and `schooling_years` are changed to `occupation`, `work`, `hours_worked`, and `schooling_years`. The data type of each column is also defined:
     - strings: state, sex, color, graduation, work, and occupation
     - int: year, quarter, and age
     - float: schooling_years, hours_worked, and income:
   - `clean_data_mod_rows`: Here the string data is treated: all letters are put in lowercase, accents are removed, and spaces at the beginning and end of words are removed. Data with `none` values are changed to ''. Finally, a new CSV is created with the treated data.
4. `upload_clean_data_to_s3`: Then, the treated data is transferred to S3 in the `pnadc_data_2023/processed` folder. (_silver layer_)
5. `transfer_s3_to_rds`: Then, the treated data is transferred to a postgres database in the `people_info` table.
6. `transfer_rds_data_to_s3`: Then, we have the construction of the _gold layer_ filtering the data present in the rds. For the data warehouse, it is necessary to collect data from women aged between 20 and 40. With the collected data, they are already saved in CSV in the `pnadc_data_2023/filter` folder.
7. `transfer_s3_to_redshift`: Then, the treated and filtered data is transferred to the `filtered_people_info_mulheres_adultas` table in redshift for querying.


# Running this project
1. To build the airflow instances, just run this command:
`docker compose up -d`
2. In airflow, you need to configure the connections:
   - ibge_api: Define the HTTP connection to access the IBGE API. The URL used is `http://servicodados.ibge.gov.br/api/v1/`
   - aws_conn: Here it is necessary to define your AWS access credentials (`aws_access_key_id` and `aws_secret_access_key`). Remember that your user in AWS needs to have access to the instances used here: S3, Redshift, and RDS.
   - postgres_conn: Here it is necessary to define `host`, `login`, `password`, and `schema` of the postgres database.
   - mongodb_conn: Here it is necessary to define `host`, `login`, and `password`. As we are using the mongodb server, it is also necessary to add `{"srv":true}` in `Extra`.
   - redshift_conn: Here it is necessary to define `host`, `login`, `password`, and `schema`.
3. Remember to run the scripts to define the tables in redshift and postgres. They are available in the [queries](https://github.com/TalissaMoura/pipeline_for_datalake_enade/tree/main/querys) folder.
4. Now just run the DAGs! The order of them here does not matter since both are acquired from different data sources.
