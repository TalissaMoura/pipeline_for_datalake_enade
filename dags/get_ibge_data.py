from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from datetime import datetime
import json
from scripts.extract import _get_data_from_ibge
from scripts.transform import _clean_rows
from scripts.send import _send_from_rds_to_s3,_send_from_s3_to_rds

today = datetime.today().strftime('%Y_%m_%d')

with DAG("get_igbe_data",start_date=datetime(2023,6,23),catchup=False,schedule=None) as dag:
    make_request = SimpleHttpOperator(
        task_id='request_data',
        http_conn_id='ibge_api',
        endpoint='/localidades/distritos',
        method='GET',
        response_filter=lambda response: json.loads(response.text),
        log_response=True
    )
    get_data = PythonOperator(
        task_id="get_ibge_data",
        python_callable=_get_data_from_ibge,
        op_kwargs = {
            'path_to_save_data':'/tmp/data_from_ibge.csv',
            'encoding':'ISO-8859-1'
        }
    )
    store_data = LocalFilesystemToS3Operator(
        task_id="upload_to_s3",
        aws_conn_id="aws_conn",
        filename = "/tmp/data_from_ibge.csv",
        dest_key="ibge/raw/geoloc_data_from_ibge.csv",
        dest_bucket="ibgebucket-data-pnadc"
    )

    clean_data = PythonOperator(
        task_id="clean_data",
        python_callable=_clean_rows,
        op_kwargs={
                  "path_to_data":f"/tmp/data_from_ibge.csv",
                  'encoding':'ISO-8859-1',
                  "path_to_save":f"/tmp/geoloc_data_igbe_processed_{today}.csv"
            }
    )

    send_data_to_s3 = LocalFilesystemToS3Operator(
        task_id = "upload_clean_data_to_s3",
        aws_conn_id="aws_conn",
        filename=f"/tmp/geoloc_data_igbe_processed_{today}.csv",
        dest_key= "ibge/processed" + f"/geoloc_data_igbe_processed_{today}.csv",
        dest_bucket="ibgebucket-data-pnadc"
    )

    send_data_to_rds = PythonOperator(
        task_id="transfer_s3_to_rds",
        python_callable=_send_from_s3_to_rds,
        op_args =[f"/tmp/geoloc_data_igbe_processed_{today}.csv",
                  "ibgebucket-data-pnadc",
                  "ibge/processed" + f"/geoloc_data_igbe_processed_{today}.csv",
                  "public",
                  "ibge_data"],
        op_kwargs = {
            "aws_conn_id":"aws_conn",
            "psql_conn_id":"postgres_conn"}
    )

    rds_to_s3_task_cites = PythonOperator(
        task_id='transfer_rds_cities_data_to_s3',
        python_callable=_send_from_rds_to_s3,
        op_args=[
            f'/tmp/filter_geoloc_data_igbe_processed_cites_{today}.csv',
            'ibgebucket-data-pnadc',
            "ibge/filter" + f"/filter_geoloc_data_igbe_processed_cities_{today}.csv",
            '''
              SELECT 
                nome,
                microrregiao,
                mesorregiao,
                uf,
                uf_sigla
              FROM ibge_data;
           '''
        ],
        op_kwargs = {
            "aws_conn_id":"aws_conn",
            "psql_conn_id":"postgres_conn"}

    )

    rds_to_s3_task_states = PythonOperator(
        task_id='transfer_rds_states_data_to_s3',
        python_callable=_send_from_rds_to_s3,
        op_args=[
            f'/tmp/filter_geoloc_data_igbe_processed_states_{today}.csv',
            'ibgebucket-data-pnadc',
            "ibge/filter" + f"/filter_geoloc_data_igbe_processed_states_{today}.csv",
            '''
              SELECT 
                DISTINCT uf,
                uf_sigla,
                regiao,
                regiao_sigla
              FROM ibge_data;
           '''
        ],
        op_kwargs = {
            "aws_conn_id":"aws_conn",
            "psql_conn_id":"postgres_conn"}

    )

    transfer_s3_to_redshift_cities = S3ToRedshiftOperator(
        task_id="transfer_s3_to_redshift_cities",
        aws_conn_id="aws_conn",
        redshift_conn_id="redshift_conn",
        s3_bucket="ibgebucket-data-pnadc",
        s3_key="ibge/filter" + f"/filter_geoloc_data_igbe_processed_cities_{today}.csv",
        schema="public",
        table="ibge_data_cities",
        copy_options=["DELIMITER ','","NULL AS ''","IGNOREHEADER 1"],
    )

    transfer_s3_to_redshift_states = S3ToRedshiftOperator(
        task_id="transfer_s3_to_redshift_states",
        aws_conn_id="aws_conn",
        redshift_conn_id="redshift_conn",
        s3_bucket="ibgebucket-data-pnadc",
        s3_key="ibge/filter" + f"/filter_geoloc_data_igbe_processed_states_{today}.csv",
        schema="public",
        table="ibge_data_states",
        copy_options=["DELIMITER ','","NULL AS ''","IGNOREHEADER 1"],
    )


    make_request >> get_data >> store_data >> clean_data >> send_data_to_s3 \
    >> send_data_to_rds >> [rds_to_s3_task_cites,rds_to_s3_task_states] \
    
    rds_to_s3_task_cites.set_downstream(transfer_s3_to_redshift_cities)
    rds_to_s3_task_states.set_downstream(transfer_s3_to_redshift_states)