import logging

from datetime import datetime
from airflow import DAG,Dataset
from airflow.decorators import task
from airflow.operators.python import is_venv_installed

log = logging.getLogger(__name__)

RAW_CENSUS_DATASET = Dataset("file://localhost/airflow/datasets/raw_census_dataset.csv")

with DAG(
    dag_id="census_data_pipeline",
    schedule=None,
    start_date=datetime(2023, 12, 9)
):
    if not is_venv_installed():
        raise RuntimeError("virutalenv not installed!")
    else:
        @task.virtualenv(
                task_id="download_rawdata_census", requirements=["pandas==2.1.1"],
                system_site_packages=False
        )
        def download_raw_data_census():
            import pandas as pd
            df = pd.read_csv("https://raw.githubusercontent.com/practical-bootcamp/week4-assignment1-template/main/city_census.csv",index_col=0)
            df.to_csv("/opt/airflow/datasets/raw_census.csv")

        @task.virtualenv(
                task_id="clean_data_census", requirements=["pandas==2.1.1"],
                system_site_packages=False
        )
        def clean_data_census():
            import pandas as pd
            df = pd.read_csv("/opt/airflow/datasets/raw_census.csv")
            df.dropna(axis='index',how='any',inplace=True)
            df = df.loc[(df['age'] > 30) & (df['state'] == 'Iowa')]
            df.to_csv("/opt/airflow/datasets/cleaned_census_dataset.csv")

        @task.virtualenv(
                task_id="persist_census_data", requirements=["pandas==2.1.1", "sqlalchemy==2.0.21"],
                system_site_packages=False
        )
        def persist_dataset():
            import pandas as pd
            import os
            from sqlalchemy.orm import sessionmaker
            import logging
            from model import Census,Base,engine

            log = logging.getLogger(__name__)

            # Create the session
            session = sessionmaker()
            session.configure(bind=engine)
            s = session()
            
            # read the data in to memory
            try:
                log.info("try to read")
                df = pd.read_csv("/opt/airflow/datasets/cleaned_census_dataset.csv", index_col =0)
                for index,row in df.iterrows():
                    record = Census(
                            **{
                                "first": row[0],
                                "last": row[1],
                                "age": row[2],
                                "state": row[3],
                                "weight": row[4],
                            }
                    )
                    s.add(record)
                s.commit() # attempt to commit all reocrds
            except Exception as e:
                log.error(f"exception {e} occured")
                s.rollback() # rollback if any error
            finally:
               s.close()

        @task.virtualenv(
                task_id="print_census_data", requirements=["pandas==2.1.1", "sqlalchemy==2.0.21"],
                system_site_packages=False
        )
        def print_data():
            from sqlalchemy import select
            from model import Census,Base,engine
            from sqlalchemy.orm import sessionmaker
            import logging
            
            log = logging.getLogger(__name__)
        
            session = sessionmaker()
            session.configure(bind=engine)
            s = session()

            stmt = select(Census)
            result = s.execute(stmt)
            
            for obj in result.scalars():
                log.info(obj.first)
            s.close()
 
        download_raw_data_census() >> clean_data_census() >> persist_dataset() >> print_data()




