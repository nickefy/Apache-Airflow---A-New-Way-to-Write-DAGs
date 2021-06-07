import os,sys,glob
from pathlib import Path
import json
from datetime import datetime, timedelta
import time

from airflow import models
from airflow.utils import trigger_rule
from airflow import DAG, AirflowException
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.print_text_old import PrintText

from airflow.models import Variable

class CreateDagOperator:

    @staticmethod
    def get_config_dictionary(config_path, conf_file):
        """
            This Method reads the job and task config files.
        """
        with open(os.path.join(config_path, conf_file), 'r') as json_file:
            config_dict = json.load(json_file)
            if str(config_dict).strip() == "":
                config_dict = {}
            if not config_dict:
                config_dict = {}
            return config_dict

    @staticmethod
    def create_dag(dag_id, default_dag_args):
        """
            Bulding JOB_PATH and CONFIG_PATH for reading the job and task configuration's.
        """
        JOB_PATH = os.path.join(AIRFLOW_HOME, "dags", dag_id)
        job_dict = CreateDagOperator.get_config_dictionary(JOB_PATH, "jobconfig.json")
        task_dict = CreateDagOperator.get_config_dictionary(JOB_PATH, "taskconfig.json")

        """
            Updating the default arguments as per job config.
        """
        job_dict_update = job_dict.copy()
        default_dag_args.update(job_dict_update)

        task_dict_update = task_dict.copy()
        task_dict.clear()

        wf_desttask = {}

        start_task = DummyOperator(task_id='start', dag=dag)

        wf_desttask['PrintText1'] = PrintText(
            task_id=task_dict_update["destination"][0]["task_name"],
            text_to_print=task_dict_update["destination"][0]["text_to_print"],
            dag=dag)

        wf_desttask['PrintText2'] = PrintText(
            task_id=task_dict_update["destination"][1]["task_name"],
            text_to_print=task_dict_update["destination"][1]["text_to_print"],
            dag=dag)

        end_task = DummyOperator(task_id='end', dag=dag)

        start_task >> wf_desttask['PrintText1'] >> wf_desttask['PrintText2'] >> end_task

        return dag