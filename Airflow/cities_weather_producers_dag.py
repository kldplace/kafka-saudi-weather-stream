from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import papermill as pm

# Run a Jupyter notebook using papermill package
def run_notebook(notebook_path, output_path):
    pm.execute_notebook(
        notebook_path,
        output_path,
        parameters={}
    )

default_args = {
    'owner': 'khalid',
    'start_date': datetime(2024, 8, 12),
    'retries': 1,
}


with DAG('saudi_weather_dag',
         default_args=default_args,
         schedule_interval=None,  
         catchup=False) as dag:

    # The tasks
    assirProducer = PythonOperator(
        task_id='Assir_producer_task',
        python_callable=run_notebook,
        op_kwargs={
            'notebook_path': '/home/kld/Github_projects/kafka-saudi-weather-stream/Kafka/Kafka-producers/kafka_Assir_producer.ipynb',
            'output_path': '/home/kld/Github_projects/kafka-saudi-weather-stream/Kafka/Kafka-producers/Output/Assir_produce_output.ipynb'
        }
    )

    jazanProducer = PythonOperator(
        task_id='Jazan_producer_task',
        python_callable=run_notebook,
        op_kwargs={
            'notebook_path': '/home/kld/Github_projects/kafka-saudi-weather-stream/Kafka/Kafka-producers/kafka_Jazan_producer.ipynb',
            'output_path': '/home/kld/Github_projects/kafka-saudi-weather-stream/Kafka/Kafka-producers/Output/Jazan_producer_output.ipynb'
        }
    )

    tabukProducer = PythonOperator(
        task_id='Tabuk_producer_task',
        python_callable=run_notebook,
        op_kwargs={
            'notebook_path': '/home/kld/Github_projects/kafka-saudi-weather-stream/Kafka/Kafka-producers/kafka_Tabuk_producer.ipynb',
            'output_path': '/home/kld/Github_projects/kafka-saudi-weather-stream/Kafka/Kafka-producers/Output/kafka_Tabuk_output.ipynb'
        }
    )
    
    bahaProducer = PythonOperator(
        task_id='Baha_producer_task',
        python_callable=run_notebook,
        op_kwargs={
            'notebook_path': '/home/kld/Github_projects/kafka-saudi-weather-stream/Kafka/Kafka-producers/kafka_Baha_producer.ipynb',
            'output_path': '/home/kld/Github_projects/kafka-saudi-weather-stream/Kafka/Kafka-producers/Output/kafka_Baha_output.ipynb'
        }
    )
    
    hailProducer = PythonOperator(
        task_id='Hail_producer_task',
        python_callable=run_notebook,
        op_kwargs={
            'notebook_path': '/home/kld/Github_projects/kafka-saudi-weather-stream/Kafka/Kafka-producers/kafka_Hail_producer.ipynb',
            'output_path': '/home/kld/Github_projects/kafka-saudi-weather-stream/Kafka/Kafka-producers/Output/kafka_Hail_output.ipynb'
        }
    )

    jawfProducer = PythonOperator(
        task_id='Jawf_producer_task',
        python_callable=run_notebook,
        op_kwargs={
            'notebook_path': '/home/kld/Github_projects/kafka-saudi-weather-stream/Kafka/Kafka-producers/kafka_Jawf_producer.ipynb',
            'output_path': '/home/kld/Github_projects/kafka-saudi-weather-stream/Kafka/Kafka-producers/Output/kafka_Jawf_output.ipynb'
        }
    )
    
    madinaProducer = PythonOperator(
        task_id='Madina_producer_task',
        python_callable=run_notebook,
        op_kwargs={
            'notebook_path': '/home/kld/Github_projects/kafka-saudi-weather-stream/Kafka/Kafka-producers/kafka_Madina_producer.ipynb',
            'output_path': '/home/kld/Github_projects/kafka-saudi-weather-stream/Kafka/Kafka-producers/Output/kafka_Madina_output.ipynb'
        }
    )
    
    meccaProducer = PythonOperator(
        task_id='Mecca_producer_task',
        python_callable=run_notebook,
        op_kwargs={
            'notebook_path': '/home/kld/Github_projects/kafka-saudi-weather-stream/Kafka/Kafka-producers/kafka_Mecca_producer.ipynb',
            'output_path': '/home/kld/Github_projects/kafka-saudi-weather-stream/Kafka/Kafka-producers/Output/kafka_Mecca_output.ipynb'
        }
    )
    
    najranProducer = PythonOperator(
        task_id='Najran_producer_task',
        python_callable=run_notebook,
        op_kwargs={
            'notebook_path': '/home/kld/Github_projects/kafka-saudi-weather-stream/Kafka/Kafka-producers/kafka_Najran_producer.ipynb',
            'output_path': '/home/kld/Github_projects/kafka-saudi-weather-stream/Kafka/Kafka-producers/Output/kafka_Najran_output.ipynb'
        }
    )
    
    
    northern_boarder_Producer = PythonOperator(
        task_id='Northern_boarder_producer_task',
        python_callable=run_notebook,
        op_kwargs={
            'notebook_path': '/home/kld/Github_projects/kafka-saudi-weather-stream/Kafka/Kafka-producers/kafka_Northern_boarder_producer.ipynb',
            'output_path': '/home/kld/Github_projects/kafka-saudi-weather-stream/Kafka/Kafka-producers/Output/kafka_Northern_boarder_output.ipynb'
        }
    )
    
    qassimProducer = PythonOperator(
        task_id='Qassim_producer_task',
        python_callable=run_notebook,
        op_kwargs={
            'notebook_path': '/home/kld/Github_projects/kafka-saudi-weather-stream/Kafka/Kafka-producers/kafka_Qassim_producer.ipynb',
            'output_path': '/home/kld/Github_projects/kafka-saudi-weather-stream/Kafka/Kafka-producers/Output/kafka_Qassim_output.ipynb'
        }
    )
    
    riyadhProducer = PythonOperator(
        task_id='Riyadh_producer_task',
        python_callable=run_notebook,
        op_kwargs={
            'notebook_path': '/home/kld/Github_projects/kafka-saudi-weather-stream/Kafka/Kafka-producers/kafka_Riyadh_producer.ipynb',
            'output_path': '/home/kld/Github_projects/kafka-saudi-weather-stream/Kafka/Kafka-producers/Output/kafka_Riyadh_output.ipynb'
        }
    )


[assirProducer, jazanProducer, tabukProducer, bahaProducer, hailProducer, jawfProducer, madinaProducer, meccaProducer, najranProducer, northern_boarder_Producer, qassimProducer, riyadhProducer]
