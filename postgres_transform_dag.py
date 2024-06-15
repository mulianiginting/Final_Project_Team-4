from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from transform_data import read_transform_data

# Pengaturan default arguments DAG
default_args = {
    'owner': 'your_username',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 6, 15),  # Tanggal awal eksekusi DAG
    'schedule_interval': '@daily',  # Penjadwalan DAG setiap hari
}

# Inisialisasi DAG
dag = DAG(
    'postgres_transform_data_dag',
    default_args=default_args,
    description='DAG to transform and load data into PostgreSQL',
    catchup=False  # Tidak menangkap (catch up) eksekusi yang tertinggal
)

# Definisikan task menggunakan PythonOperator
transform_data_task = PythonOperator(
    task_id='transform_data_task',
    python_callable=read_transform_data,
    dag=dag,
)

# Anda dapat menambahkan lebih banyak task di sini jika diperlukan
# Misalnya, jika Anda memiliki task lain yang perlu dijalankan setelah transformasi data

if __name__ == "__main__":
    dag.cli()

