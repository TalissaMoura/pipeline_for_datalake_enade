import pandas as pd
import sqlalchemy
from airflow.hooks.S3_hook import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

def _send_from_s3_to_rds(
        path_to_file,
        bucket_name,
        key,
        schema,
        name_table,
        aws_conn_id="aws_default",
        psql_conn_id="postgres_default",
        if_exists="replace",
        chunksize=10000,
        index=True,
        index_label=None,
        method="multi"
):
   hook_s3 = S3Hook(aws_conn_id=aws_conn_id)
   client_s3 = hook_s3.get_conn()
   client_s3.download_file(bucket_name,key,path_to_file)

   hook_psql = PostgresHook(postgres_conn_id=psql_conn_id)
   uri_psql = hook_psql.get_uri()
   engine_psql = sqlalchemy.create_engine(uri_psql)

   df = pd.read_csv(path_to_file,keep_default_na=False)

   df.to_sql(
      schema=schema,
      name=name_table,
      con=engine_psql,
      if_exists=if_exists,
      chunksize=chunksize,
      index=index,
      index_label=index_label,
      method=method
   )

   print(f"End to add rows! {df.shape[0]} rows added.")


def _send_from_rds_to_s3(
        filename,
        bucket_name,
        key,
        query,
        replace_null='',
        aws_conn_id="aws_default",
        psql_conn_id="postgres_default",
):
   hook_s3 = S3Hook(aws_conn_id=aws_conn_id)
   client_s3 = hook_s3.get_conn()

   hook_psql = PostgresHook(postgres_conn_id=psql_conn_id)
   uri_psql = hook_psql.get_uri()
   engine_psql = sqlalchemy.create_engine(uri_psql)

   df = pd.read_sql(query,con=engine_psql)

   ## fillna with replace_null // Redshift doesn't reconize NaN as null
   df = df.fillna(value=replace_null)

   ## save csv
   df.to_csv(filename,index=False)

   client_s3.upload_file(filename,bucket_name,key)
   
    