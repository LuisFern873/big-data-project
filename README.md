# Sistema de Análisis Táctico de Partidos de Fútbol mediante Big Data

## Quick setup:

Construye la imagen docker:

```shell
docker build -t my-airflow:latest .
```

Levanta los contenedores:

```shell
docker compose up -d
```

Esperar un momento hasta que todo se levante correctamente.


Accedemos a Apache Airflow por medio de: http://localhost:8080 

En caso se solicite credenciales:
- user: admin
- password: admin


Podemos visualizar nuestras bases de datos y colecciones de MongoDB en: http://localhost:8081

## DAGs

Antes de ejecutar los DAGs, asegurate de cargar el archivo `copa32_events.json` en el directorio `airflow/data`.

- `extract_transform_load_dag.py`: extrae los datos de `copa32_events.json`, realiza transformaciones y finalmente los carga a la base de datos `copa2024` en MongoDB.

- `compute_kpis_dag.py`: calcula KPIs posiblemente útiles para el dashboard. Se ejecuta solo cuando los datos ya están cargados en MongoDB.

Al ejecutar los DAGs se generan archivos en `airflow/logs`.


## Dashboard

```shell
streamlit run dashboard/stream.py
```

