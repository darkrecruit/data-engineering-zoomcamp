gcloud dataproc jobs submit pyspark \
    --cluster=cluster-cd95 \
    --region=us-central1 \
    --properties spark.master=local[*] \
    gs://de-zoomcamp-nytaxi-project-d79af39f-8a71-4f5d-812/06_spark_sql_big_query.py \
    -- \
        --input_green=gs://de-zoomcamp-nytaxi-project-d79af39f-8a71-4f5d-812/pq/green/2020 \
        --input_yellow=gs://de-zoomcamp-nytaxi-project-d79af39f-8a71-4f5d-812/pq/yellow/2020 \
        --output=trips_data_all.reports_2020
