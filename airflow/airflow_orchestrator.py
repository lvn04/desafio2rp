from airflow import DAG
from airflow.operators.bash import BashOperator

from datetime import datetime, timedelta

# ===============================================================================

# Criando parametro de data para rodar a dag
ontem = datetime.today() - timedelta(days=1)

# formato: YYYYM
ano = ontem.strftime("%Y")
mes = str(int(ontem.strftime("%m")))
data_parametro = ano + mes

# ===============================================================================

# Definindo DAG

dag = DAG(
    dag_id="prova",
    start_date=datetime(2023, 1, 17),
    # rodar uma ver por dia a 00:30
    schedule_interval="0 0 1 * *",
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
    default_args={
        "owner": "airflow",
        "depends_on_past": False
    }
)

# ===============================================================================
bronze = "/bronze/shell_files/"
silver = "/silver/shell_files/"
gold = "/gold/shell_files/"

# ===============================================================================

task_ingestion_bronze = BashOperator(
    task_id="ingestion_bronze",
    bash_command=f"spark-submit {bronze}ingestion.sh",
    # Pass the parameter date to the script
    env={"date_parameter": data_parametro},
    dag=dag
)


task_prescribers_silver = BashOperator(
    task_id="backup_bronze",
    bash_command=f"spark-submit {silver}prescribers.sh",
    # Pass the parameter date to the script
    env={"date_parameter": data_parametro},
    dag=dag
)

task_prescriptions_silver = BashOperator(
    task_id="backup_bronze",
    bash_command=f"spark-submit {silver}prescriptions.sh",
    # Pass the parameter date to the script
    env={"date_parameter": data_parametro},
    dag=dag
)


task_prescriptions_prescribers_gold = BashOperator(
    task_id="aerodromos_bronze",
    bash_command=f"spark-submit {gold}prescriptions_prescribers.sh",
    # Pass the parameter date to the script
    env={"date_parameter": data_parametro},
    dag=dag
)

# ===============================================================================

task_ingestion_bronze >> task_prescribers_silver >> task_prescriptions_silver >> task_prescriptions_prescribers_gold
