#Nova Taskflow API
from airflow.decorators import dag, task
from datetime import datetime, timedelta

default_args = {
    'owner': 'Tiago',
    'depends_on_past':False,
    'start_date': datetime(2023,1,1)
} 

@dag(default_args=default_args, schedule_interval="@once", description="Nova TaskFlow API", catchup=False, tags=['taskflowAPI'])
def exercicio2():

    @task
    def start():
        print("Começou!!")
    
    @task
    def say_today():
        agora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(agora)
        return agora
    
    @task
    def cinco_dias_mais(dataref):
        dataref = datetime.strptime(dataref, "%Y-%m-%d %H:%M:%S")
        datanova = dataref + timedelta(days=5)
        print(datanova.strftime("%Y-%m-%d %H:%M:%S"))

    @task
    def cinco_dias_menos(dataref):
        dataref = datetime.strptime(dataref, "%Y-%m-%d %H:%M:%S")
        datanova = dataref - timedelta(days=5)
        print(datanova.strftime("%Y-%m-%d %H:%M:%S"))
    
    @task
    def end():
        print("Terminouuuuuuu!")

    #Orquestar
    st = start()
    ed = end()
    today = say_today()
    cma = cinco_dias_mais(today)
    cme = cinco_dias_menos(today)

    st>>today
    [cma,cme]>>ed

execucao =exercicio2()