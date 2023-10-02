#utilizando a nuvem AWS
from airflow.decorators import task, dag
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
from datetime import datetime
import boto3

aws_access_key_id = Variable.get("aws_access_key_id")
aws_secret_access_key = Variable.get("aws_secret_access_key")

client = boto3.client(
    'emr',
    region_name='us-east-1',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key
)

default_args = {
    'owner':'Tiago',
    'start_date': datetime(2023,1,1)
}

@dag(default_args=default_args, schedule_interval="@once", catchup=False, tags=['Spark','EMR'])
def indicadores_titanic():
    inicio =DummyOperator(task_id='inicio')

    @task
    def emr_process_titanic():
        newstep = client.add_job_flow_steps(
            JobFlowId="ID DO CLUSTER AWS",
            Steps=[
                {
                    'Name':'Processa Indicadores TITANIC',
                    'ActionOnFailure':'CONTINUE',
                    'HadoopJarStep':{
                        'Jar':'command-runner.jar'
                        'Args':[
                            'spark-submit',
                            '--master','yarn',
                            '--deploy-mode','cluster',
                            '--packages', 'io.delta:delta-core_2.12:2.1.0',
                            '<URI DO CODIGO SPARK NO S3>' #ALTERAR
                        ]
                    }
                }
            ]
        )
        return newstep['Step_ID'][0]
    
    @task
    def wait_emr_job(stepID):
        waiter = client.get_waiter('step_complete')
        waiter.wait(
            ClusterId = "ID DO CLUSTER AWS",
            StepID=stepID,
            WaiterConfig={
                'Delay':10,
                'MaxAttempts':300
            }
        )
    
    fim=DummyOperator(task_id='fim')

    #ORQUESTRAÇÂO
    processo = emr_process_titanic()
    espera = wait_emr_job(processo)

    inicio>>processo
    espera>>fim

execucao = indicadores_titanic()