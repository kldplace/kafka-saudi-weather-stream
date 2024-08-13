## Project Overview
![Kafka-project-diagram](/img/kafka-project-diagram.png)


This project simulates near real-time weather data streaming in Saudi cities using Apache Kafka. Each city functions as a Kafka producer, sending weather updates every two seconds to Kafka topics. The original dataset, sourced from Kaggle [Saudi Arabia Weather History](https://www.kaggle.com/datasets/esraamadi/saudi-arabia-weather-history), provides hourly weather updates. For this simulation, the dataset has been separated into 12 datasets, each representing a different Saudi city, to simulate 12 parallel Kafka producers.

![airflow](/img/airflow_dag.png)

Apache Airflow orchestrates the simulation, ensuring all producers run in parallel. The data is then consumed and stored in an S3 bucket, where AWS Glue crawlers organize it for analysis using AWS Athena.

![s3Bucket-result](/img/event_log_s3Bucket.png)

## Kafka Architecture and Data Flow
![kafka-architecture](/img/project-architecture.png)

I use Apache Kafka to simulate real-time weather data streaming from 12 Saudi cities. I created a **`Kafka Topic: saudi_weather_topic`** with **`3 Partitions`**.

```
kafka-topics.sh --bootstrap-server ip:9092 --topic saudi_weather_topic --create --partitions 3
```
With 12 producers generating records every two seconds, Kafka's 3 partitions allow for efficient parallel processing. Kafka tracks the consumer's position in the data stream using offsets, which are updated as messages are read. The processed data is stored in an S3 bucket, with offsets ensuring that each piece of data is processed and saved correctly. This setup balances the load across partitions and enhances fault tolerance, ensuring continuous data flow even if one partition fails.