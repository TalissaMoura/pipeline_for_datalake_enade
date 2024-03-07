from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.hooks.S3_hook import S3Hook
import json
import pandas as pd
from collections import defaultdict


def _get_mongodb_data(db_name,collection_name,path_to_save,mongo_conn_name="mongo_default"):
    data = []
    hook = MongoHook(conn_id=mongo_conn_name)
    client = hook.get_conn()
    db = client.get_database(db_name)
    col = db.get_collection(collection_name)
    raw_data = col.find({})
    
    for d in raw_data:
       new_dict  = {k:v for k,v in d.items() if k!="_id"}
       data.append(new_dict)
    
    with open(path_to_save,'w') as f:
      f.write(json.dumps(data))
    
 

def _get_raw_data_from_s3(bucket_name,key,path_to_save,aws_conn_id="aws_default"):
   hook = S3Hook(aws_conn_id=aws_conn_id)
   client = hook.get_conn()
   client.download_file(bucket_name,key,path_to_save)


def _get_data_from_ibge(ti,path_to_save_data,encoding='utf-8'):
    res_json = ti.xcom_pull(task_ids="request_data")
    raw_data = []
    for req in res_json:
       data = defaultdict(str)
       data['nome']=req['nome']
       data['microrregiao']=req['municipio']['microrregiao']['nome']
       data['mesorregiao']=req['municipio']['microrregiao']['mesorregiao']['nome']
       data['uf']=req['municipio']['microrregiao']['mesorregiao']['UF']['nome']
       data['uf_sigla']=req['municipio']['microrregiao']['mesorregiao']['UF']['sigla']
       data['regiao']=req['municipio']['microrregiao']['mesorregiao']['UF']['regiao']['nome']
       data['regiao_sigla']=req['municipio']['microrregiao']['mesorregiao']['UF']['regiao']['sigla']
       raw_data.append(data)
    
    df = pd.DataFrame(raw_data)
    
    df.to_csv(path_to_save_data,encoding=encoding,index=False)