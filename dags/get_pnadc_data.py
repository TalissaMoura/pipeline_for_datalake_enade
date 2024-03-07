from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from scripts.extract import _get_mongodb_data,_get_raw_data_from_s3
from scripts.transform import _clean_data
from scripts.send import _send_from_s3_to_rds,_send_from_rds_to_s3
from datetime import datetime
import pandas as pd


today = datetime.today().strftime('%Y_%m_%d')

def parse_csv_to_list(filepath):
    values = pd.read_csv(filepath,keep_default_na=False,decimal=",").values
    return values

with DAG("get_pnadc_data",start_date=datetime(2023,6,24),
         catchup=False,schedule=None) as dag:
    
    get_data_from_mongo = PythonOperator(
        task_id="get_mongodb_data",
        python_callable=_get_mongodb_data,
        op_args =["ibge","pnadc20203",f"/tmp/pnadc_data_{today}.json"],
        op_kwargs = {
            "mongo_conn_name":"mongodb_conn"}
    )

    store_data_to_s3 = LocalFilesystemToS3Operator(
        task_id="upload_raw_data_to_s3",
        aws_conn_id="aws_conn",
        filename = f"/tmp/pnadc_data_{today}.json",
        dest_key="pnadc_data_2023/raw"+f"/pnadc_data_{today}.json",
        dest_bucket="ibgebucket-data-pnadc"
    )
    get_data_from_s3 = PythonOperator(
        task_id="get_s3_raw_data",
        python_callable=_get_raw_data_from_s3,
        op_args=["ibgebucket-data-pnadc","pnadc_data_2023/raw" + f"/pnadc_data_{today}.json",
                 f"/tmp/raw_data_{today}.json"],
        op_kwargs ={
            "aws_conn_id":"aws_conn",
        }
        )
    
    cleans = _clean_data(date=today)

    send_data_to_s3 = LocalFilesystemToS3Operator(
        task_id = "upload_clean_data_to_s3",
        aws_conn_id="aws_conn",
        filename=f"/tmp/processed_data_pnadc_end_{today}.csv",
        dest_key= "pnadc_data_2023/processed" + f"/processed_data_pnadc_{today}.csv",
        dest_bucket="ibgebucket-data-pnadc"
    )

    send_data_to_rds = PythonOperator(
        task_id="transfer_s3_to_rds",
        python_callable=_send_from_s3_to_rds,
        op_args =["/tmp/processed_pnadc_data_{today}.csv",
                  "ibgebucket-data-pnadc",
                  "pnadc_data_2023/processed" + f"/processed_data_pnadc_{today}.csv",
                  "public",
                  "people_info"],
        op_kwargs = {
            "aws_conn_id":"aws_conn",
            "psql_conn_id":"postgres_conn"}
    )

    rds_to_s3_task = PythonOperator(
        task_id='transfer_rds_data_to_s3',
        python_callable=_send_from_rds_to_s3,
        op_args=[
            f'/tmp/filter_data_pnadc_data_{today}.csv',
            'ibgebucket-data-pnadc',
            "pnadc_data_2023/filter" + f"/filter_processed_data_pnadc_{today}.csv",
            '''
              SELECT * FROM people_info
              WHERE sexo ='mulher'
              AND idade BETWEEN 20 AND 40;
           '''
        ],
        op_kwargs = {
            "aws_conn_id":"aws_conn",
            "psql_conn_id":"postgres_conn"}

    )
    transfer_s3_to_redshift = S3ToRedshiftOperator(
        task_id="transfer_s3_to_redshift",
        aws_conn_id="aws_conn",
        redshift_conn_id="redshift_conn",
        s3_bucket="ibgebucket-data-pnadc",
        s3_key=f"pnadc_data_2023/filter" + f"/filter_processed_data_pnadc_{today}.csv",
        schema="public",
        table="filtered_people_info_mulheres_adultas",
        copy_options=["DELIMITER ','","NULL AS ''","IGNOREHEADER 1"],
    )


    get_data_from_mongo >> store_data_to_s3 >> get_data_from_s3 \
    >> cleans >> send_data_to_s3 >> send_data_to_rds >> rds_to_s3_task \
    >> transfer_s3_to_redshift