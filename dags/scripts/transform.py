import unicodedata
import pandas as pd
import numpy as np
import json
from airflow.utils.task_group import TaskGroup
from airflow.operators.python_operator import PythonOperator


def _clean_columns(path_json_data=None,df=None,orient_of_data="records",save_data=True,path_to_save=None,encoding='utf-8'):
         if isinstance(df,pd.DataFrame):
              df = df.copy()
         else:
            df = pd.read_json(path_json_data,orient=orient_of_data)
         
         ## rename columns
         df = df.rename({
           "ocup":"ocupacao",
           "trab":"trabalho",
           "horastrab":"horas_trabalhadas",
           "anosesco":"anos_escolaridade"
           },axis=1
           )

         ## define type 
         df = df.astype({
            "uf":"str",
            "sexo":"str",
            "cor":"str",
            "graduacao":"str",
            "trabalho":"str",
            "ocupacao":"str",
            "anos_escolaridade":"float",
            "horas_trabalhadas":"float",
            "renda":"float",
            "ano":"int",
            "trimestre":"int",
            "idade":"int"
            })
         if save_data and path_to_save != None:
            df.to_csv(path_to_save,index=False,encoding=encoding)
         else:
            return df

def _clean_rows(path_to_data=None,df=None,save_data=True,path_to_save=None,encoding='utf-8'):
       if isinstance(df,pd.DataFrame):
            df = df.copy()
       else:
           df = pd.read_csv(path_to_data,encoding=encoding)
       
       df_str = df.select_dtypes(include='object')
       str_columns = df_str.columns
       str_values = df_str.values
       
       # ##clean chars with accents and lower words 
       # ##lower words
       lower = np.vectorize(lambda s: s.lower())
       str_values = lower(str_values)
       df.loc[:,str_columns] = str_values

       # ##remove spaces
       rm_spcs = np.vectorize(lambda s: s.strip())
       str_values = rm_spcs(str_values)
       df.loc[:,str_columns] = str_values

       ## replace 'none' values
       str_values = np.where(str_values=='none','',str_values)
       df.loc[:,str_columns] = str_values

       # ##clean accents
       normalize_wrds = np.vectorize(lambda s: unicodedata.normalize("NFD",s))
       str_values = normalize_wrds(str_values)
       encode_asc = np.vectorize(lambda s: s.encode("ascii","ignore"))
       str_values = encode_asc(str_values)
       decode_utf8 = np.vectorize(lambda s: s.decode("utf-8"))
       str_values = decode_utf8(str_values)
       df.loc[:,str_columns]=str_values
       

       if save_data and path_to_save != None:
          df.to_csv(path_to_save,index=False,na_rep='')
       else:
          return df


def _clean_data(date="1970_1_1_10_11_10"):
    with TaskGroup('cleans',tooltip='cleaning tasks') as group:
        clean_raw_json_columns = PythonOperator(
             task_id="clean_data_rename_cols",
             python_callable= _clean_columns,
             op_kwargs = {
                  "path_json_data":f"/tmp/raw_data_{date}.json",
                  "orient_of_data":"records",
                  "path_to_save": f"/tmp/processed_data_pnadc_{date}.csv",
                  "encoding":"ISO-8859-1"
                  }
        )
        clean_raw_csv_rows = PythonOperator(
            task_id="clean_data_mod_rows",
            python_callable=_clean_rows,
            op_kwargs={
                  "path_to_data":f"/tmp/processed_data_pnadc_{date}.csv",
                  "path_to_save":f"/tmp/processed_data_pnadc_end_{date}.csv",
                  "encoding":"ISO-8859-1"
            }
         )
        
        clean_raw_json_columns >> clean_raw_csv_rows
    
    return group
            

