from airflow import DAG
from datetime import datetime
from datetime import date
# Operators; we need this to operate!
from airflow.operators.python_operator import PythonOperator
from cleaning import clean_wh, clean_life_exp, clean_250_cnt, merge_data

# step 2 - define default args
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
  'owner': 'airflow',
  'depends_on_past': False,
  'start_date': datetime.now()
  }


# step 3 - instantiate DAG
dag = DAG(
  'covid-DAG',
  default_args=default_args,
  description='Fetch covid data from API',
  schedule_interval='@once',
)

# step 4 Define tasks
def store_data(**context):
  df = context['task_instance'].xcom_pull(task_ids='translate_data')
  df.to_csv("data/output.csv")


def translate_data(**context):
  [df_wrld_happi, df_250_cnt, df_life_exp] = context['task_instance'].xcom_pull(task_ids='extract_data')
  df_wrld_happi_cleaned = clean_wh(df_wrld_happi)
  df_life_exp_cleaned = clean_life_exp(df_life_exp)
  df_250_cnt_cleaned = clean_250_cnt(df_250_cnt)
  df_merged = merge_data(df_wrld_happi_cleaned, df_life_exp_cleaned, df_250_cnt_cleaned)
  return df_merged


def extract_data(**kwargs):
  df_wrld_happi = pd.read_csv('data/df_wrld_happi.csv')
  df_life_exp = pd.read_csv('data/df_life_exp.csv')
  df_250_cnt = pd.read_csv('data/df_250_cnt.csv')
  return [df_wrld_happi, df_250_cnt, df_life_exp]


t1 = PythonOperator(
  task_id='extract_data',
  provide_context=True,
  python_callable=extract_data,
  dag=dag,
)

t2 = PythonOperator(
  task_id='clean_data',
  provide_context=True,
  python_callable=clean_data,
  dag=dag,
)

t3 = PythonOperator(
  task_id='store_data',
  provide_context=True,
  python_callable=store_data,
  dag=dag,
)

# step 5 - define dependencies
t1 >> t2 >> t3
