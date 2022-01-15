# Trending topics tweet extraction

<p>
<img alt="Docker" src="https://img.shields.io/badge/docker-%230db7ed.svg?&style=for-the-badge&logo=docker&logoColor=white"/>
<img alt="Apache Airflow" src="https://img.shields.io/badge/apacheairflow-%23017CEE.svg?&style=for-the-badge&logo=apacheairflow&logoColor=white"/>
<img alt="Apache Spark" src="https://img.shields.io/badge/apachespark-%23E25A1C.svg?&style=for-the-badge&logo=apachespark&logoColor=white"/>
<img alt="Twitter" src="https://img.shields.io/badge/twitter-%231DA1F2.svg?&style=for-the-badge&logo=twitter&logoColor=white"/>
<img alt="DeltaLake" src="https://img.shields.io/badge/delta-%23003366.svg?&style=for-the-badge&logo=delta&logoColor=white"/>
</p>

This project collect tweets from Twitter with keywords specified in a YAML config file, threats it with PySpark and store with DeltaLake in three data layers. The first one, stores the data as it is collected, in batches with JSON format and GZIP compression. The second, prepares it in Parquet with Delta format partitioned by execution date (from the DAG run). At the end, the third layer stores the data also in Delta format, but partitioned by the tweet creation timestamp. The task orchestration runs at Apache Airflow with PySpark jobs implemented in PythonOperators.

At the config file, the following fields can be specified:

```yaml
topics:
    covid19:
        start-date: 2022-01-10T00:00:00-03
        schedule-interval: "*/15 * * * *"
        max-results: 50
        landing-path: /data/landing
        raw-path: /data/raw
        trusted-path: /data/trusted
```

## How to start

The Makefile wraps some docker commands to start the project. In example, to start the Apache Airflow environment, run the following:

```shell
make start
```

To run the unit tests, run the following make target:

```shell
make test
```
