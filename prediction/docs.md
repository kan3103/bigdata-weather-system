# Preprocessing
## Set up Spark
### Docker network
Create a network to interact between every components through docker

Run:
`docker network create spark-network`

### Master node
Create a master node for spark
``` bash
docker run -d \
  --name spark-master \
  --hostname spark-master \
  --network spark-network \
  -p 8080:8080 -p 7077:7077 \
  apache/spark:4.1.0-preview3-scala2.13-java21-python3-ubuntu \
  /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master
```

### Worker node
Create a worker node for Spark
``` bash
docker run -d \
 --name spark-worker-1 \
 --network spark-network \
 -v "$(pwd)/../data_in:/data_in:ro" \
 -v "$(pwd)/../data_out:/data_out" \
 apache/spark:4.1.0-preview3-scala2.13-java21-python3-ubuntu \
 /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker spark://spark-master:7077
```

### Build task to preprocessing
Run and build docker file to build job to preprocessing

``` bash
cd preprocessing
docker build -t spark-preprocessing 
```

## Running job preprocessing
### Structure data
Raw data and generated artifacts live under `prediction/datasets`:

- `prediction/datasets/data_in/`: raw CSV files consumed by Spark (e.g. `train_data/testset.csv`). Mount this directory read-only in the worker container so the job cannot mutate the source data.
- `prediction/datasets/data_out/`: Parquet shards created by the preprocessing job. The repo keeps an empty `.keep` file here so the folder structure remains in Git, while actual outputs stay untracked (`.gitignore` ignores everything else).

If you introduce new splits (validation/test), mirror the same layout inside `data_in/` and `data_out/` to keep mounts and scripts consistent.

```
prediction/
└── datasets/
    ├── data_in/
    │   ├── .keep
    │   └── train_data/
    │       └── testset.csv
    └── data_out/
        ├── .keep
        └── preprocessed_data_train/
            ├── _SUCCESS
            └── part-*.parquet
```

### Run task

Run in terminal with parameters `DATA_IN`, `DATA_OUT`, `LIST_ATTRIBUTES`

``` bash
docker run --rm \
  --name spark-job-01 \
  --network spark-network \
  -v "$(pwd)/../data_in:/data_in:ro" \
  -v "$(pwd)/../data_out:/data_out" \
  spark-preprocessing  \
  /opt/spark/bin/spark-submit /spark.py \
  --input {DATA_IN} \
  --output-dir {DATA_OUT} \
  --features {LIST_ATTRIBUTES} \
  --window-size 24
```

### Result

The preprocessing job validates the dataset (dropping nulls, normalizing timestamps, imputing missing values) and builds feature sequences covering the previous 24 hours. These sequences feed the LSTM model to forecast short-term weather targets, including 15-minute and 20-minute horizons.


# Prediction