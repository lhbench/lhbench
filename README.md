# Introduction

LHBench is a benchmark for [LakeHouse](https://www.cidrdb.org/cidr2021/papers/cidr2021_paper17.pdf) storage systems. These systems extend traditional Data Lake storage with support for ACID transactions and enable a wide range of workloads from data warehousing to machine learning in one place. LakeHouse architectures are widely adopted in industry at companies such as Uber, Meta, and Netflix. Our benchmark runs on AWS EMR and currently supports three LakeHouse storage engines: [Apache Iceberg](http://iceberg.apache.org), [Apache Hudi](http://hudi.apache.org), and [Delta Lake](http://delta.io). This benchmark is meant to enable simple and reproducible comparison of storage engines in this rapidly moving space.

# Workload
Our goal with this benchmark is to study end-to-end workloads and understand the technical differences between LakeHouse systems. We chose to adapt the existing TPC-DS data warehouse benchmark to the LakeHouse setting wherever possible to exercise these differences. This benchmark is made up of four tests, three of which use the TPC-DS dataset and a subset of the TPC-DS queries. The last test, micro merge, performs inserts and updates to a synthetic dataset at a fine granularity. The TPC-DS Refresh and Micro Merge tests are meant to exercise differences in UPDATE implementations, Large File Count TPC-DS exercises differences in metadata handling, and TPC-DS exercises end-to-end performance.

The benchmark is made up of four tests:
* 3TB TPC-DS
* Large File Count TPC-DS
* TPC-DS Refresh
* Micro Merge Comparison

# Results | December 2022
All experiments were run using Apache Spark on AWS EMR 6.9.0 storing data in AWS S3 using Delta Lake 2.2.0, Apache Hudi 0.12.0, and Apache Iceberg 1.1.0. This version of EMR is based on Apache Spark 3.3.0. More details on these runs can be found in the paper (TODO link to paper when available).
				
## 3TB TPC-DS Results

Our end-to-end comparison of Delta, Hudi, and Iceberg with 3TB TPC-DS runs each query three times and reports the median runtime. The results show Hudi is almost ten times slower for data load. This is because Hudi is optimized for keyed upserts, not bulk data ingestion, and does expensive pre-processing during data loading including key uniqueness checks and key redistribution. Our query runs showed that overall, TPC-DS ran 1.4× faster on Delta Lake than on Hudi and 1.7× faster on Delta Lake than on Iceberg. We highlight some of the specific queries where the differences were pronounced. Upon investigation we found that the query execution time differences are explained almost entirely by data reading time. You can find a more detailed discussion in the [paper](https://www.cidrdb.org/cidr2023/papers/p92-jain.pdf).
					
## Large File Count TPC-DS Results

The large file count test compares metadata processing strategies across LakeHouses. We break the store_sales TPC-DS table up into 10MB files and experiment with 1,000 through 200,000 files. We see better performance from Delta Lake for large file counts. Performance is 7x-20x better for Delta in the 200,000 files case.

## TPC-DS Refresh Results

This test first loads the 100 GB TPC-DS base dataset, then runs five sample queries (Q3, Q9, Q34, Q42, and Q59). It then runs a total of 10 refreshes (each for 3% of the original dataset) using the MERGE INTO operation to update rows. Finally, it reruns the five sample queries on the updated tables. Due to S3 connection timeouts in Iceberg 1.1.0 MoR we also show results for Iceberg 0.14.0 MoR. 		 	 	 				
We observed that merges in Hudi MoR were 1.3× faster than in Hudi CoW at the cost of 3.2× slower queries post-merge. Both Hudi CoW and MoR had poor write performance during the initial load due to additional pre-processing to distribute the data by key and rebalance write file sizes. Delta’s performance on both merges and reads was competitive, despite using only CoW, due to a combination of generating fewer files, faster scans, and a more optimized MERGE command. Merges in Iceberg version 0.14.0 with MoR were 1.4× faster than CoW. Post-merge query performance remains similar between table modes. 
					
## Micro Merge Results

The micro merge test uses a synthetic dataset of four columns in a single table. In this run we have generated a 100GB data set and we compare merge time and post-merge query time for a range of update sizes. The update sizes range from 0.0001% of the data set size to 0.1% of the data set size. We see that MoR merge time starts to outperform CoW at 100,000 rows updated. We also see, as expected, that the query latency after a merge is much higher for merge-on-read than copy-on-write due to read amplification.

# Benchmarks 

## Overview
This is a basic framework for writing benchmarks to measure Delta's performance. It is currently designed to run benchmark on Spark running in an EMR or a Dataproc cluster. However, it can be easily extended for other Spark-based benchmarks. To get started, first download/clone this repository in your local machine. Then you have to set up a cluster and run the benchmark scripts in this directory. See the next section for more details.

## Running TPC-DS benchmark

This TPC-DS benchmark is constructed such that you have to run the following two steps. 
1. *Load data*: You have to create the TPC-DS database with all the Delta tables. To do that, the raw TPC-DS data has been provided as Apache Parquet files. In this step you will have to use your EMR or a Dataproc cluster to read the parquet files and rewrite them as Delta tables.
2. *Query data*: Then, using the tables definitions in the Hive Metatore, you can run the 99 benchmark queries.   

The next section will provide the detailed steps of how to setup the necessary Hive Metastore and a cluster, how to test the setup with small-scale data, and then finally run the full scale benchmark.

### Configure cluster with Amazon Web Services

#### Prerequisites
- An AWS account with necessary permissions to do the following:
  - Manage RDS instances for creating an external Hive Metastore
  - Manage EMR clusters for running the benchmark
  - Read and write to an S3 bucket from the EMR cluster
- A S3 bucket which will be used to generate the TPC-DS data.
- A machine which has access to the AWS setup and where this repository has been downloaded or cloned.

#### Create external Hive Metastore using Amazon RDS
Create an external Hive Metastore in a MySQL database using Amazon RDS with the following specifications:
- MySQL 8.x on a `db.m5.large`.
- General purpose SSDs, and no Autoscaling storage.
- Non-empty password for admin
- Same region, VPC, subnet as those you will run the EMR cluster. See AWS docs for more guidance.
  - *Note:* Region us-west-2 since that is what this benchmark has been most tested with.

After the database is ready, note the JDBC connection details, the username and password. We will need them for the next step. Note that this step needs to be done just once. All EMR clusters can connect and reused this Hive Metastsore. 
  
#### Create EMR cluster
Create an EMR cluster that connects to the external Hive Metastore.  Here are the specifications of the EMR cluster required for running benchmarks.
- EMR with Spark and Hive (needed for writing to Hive Metastore). Choose the EMR version based on the Spark version compatible with the format. In the paper, we used EMR 6.9.0.
- Master - i3.2xlarge
- Workers - 16 x i3.2xlarge (or just 1 worker if you are just testing by running the 1GB benchmark).
- Hive-site configuration to connect to the Hive Metastore. See [Using an external MySQL database or Amazon Aurora](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-hive-metastore-external.html) for more details.
- Same region, VPC, subnet as those of the Hive Metastore.
  - *Note:* Region us-west-2 since that is what this benchmark has been most tested with.
- No autoscaling, and default EBS storage.

Once the EMR cluster is ready, note the following: 
- Hostname of the EMR cluster master node.
- PEM file for SSH into the master node.
These will be needed to run the workloads in this framework. 

#### Prepare S3 bucket
Create a new S3 bucket (or use an existing one) which is in the same region as your EMR cluster.

_________________

### Configure cluster with Google Cloud Platform

#### Prerequisites
- A GCP account with necessary permissions to do the following:
  - Manage Dataproc clusters for running the benchmark
  - Manage Dataproc Metastore instances
  - Read and write to a GCS bucket from the Dataproc cluster
- A GCS bucket which will be used to generate the TPC-DS data.
- A machine which has access to the GCP setup and where this repository has been downloaded or cloned.
- SSH keys for a user which will be used to access the master node. The user's SSH key can be either [a project-wide key](https://cloud.google.com/compute/docs/connect/add-ssh-keys#add_ssh_keys_to_project_metadata) 
  or assigned to the [master node](https://cloud.google.com/compute/docs/connect/add-ssh-keys#after-vm-creation) only.
- Ideally, all GCP components used in benchmark should be in the same location (Storage bucket, Dataproc Metastore service and Dataproc cluster).

#### Prepare GCS bucket
Create a new GCS bucket (or use an existing one) which is in the same region as your Dataproc cluster.

#### Create Dataproc Metastore
You can create [Dataproc metastore](https://cloud.google.com/dataproc-metastore/docs/create-service)
either via Web Console or gcloud command.

Sample create command:
```bash
gcloud metastore services create dataproc-metastore-for-benchmarks \
    --location=<region> \
    --tier=enterprise
```

#### Create Dataproc cluster
Here are the specifications of the Dataproc cluster required for running benchmarks.
- Image version >= 2.0 having Apache Spark 3.1
- Master - n2-highmem-8 (8 vCPU, 64 GB memory)
- Workers - 16 x n2-highmem-8 (or just 2 workers if you are just testing by running the 1GB benchmark).
- The cluster connects to the Dataproc Metastore.
- Same region and subnet as those of the Dataproc Metastore and GCS bucket.
- No autoscaling.

Sample create command:
```bash
gcloud dataproc clusters create delta-performance-benchmarks-cluster \
    --project <project-name> \
    --enable-component-gateway \
    --region <region> \
    --zone <zone> \
    --subnet default \
    --master-machine-type n2-highmem-8 \
    --master-boot-disk-type pd-ssd \
    --master-boot-disk-size 100 \
    --num-master-local-ssds 4 \
    --master-local-ssd-interface NVME \
    --num-workers 16 \
    --worker-machine-type n2-highmem-8 \
    --worker-boot-disk-type pd-ssd \
    --worker-boot-disk-size 100 \
    --num-worker-local-ssds 4 \
    --worker-local-ssd-interface NVME \
    --dataproc-metastore projects/<project-name>/locations/<region>/services/dataproc-metastore-for-benchmarks \
    --enable-component-gateway \
    --image-version 2.0-debian10
```

#### Input data
The benchmark is run using the raw TPC-DS data which has been provided as Apache Parquet files. There are two
predefined datasets of different size, 1GB and 3TB, located in `s3://devrel-delta-datasets/tpcds-2.13/tpcds_sf1_parquet/`
and `s3://devrel-delta-datasets/tpcds-2.13/tpcds_sf3000_parquet/`, respectively. Please keep in mind that
`devrel-delta-datasets` bucket is configured as [Requester Pays](https://docs.aws.amazon.com/AmazonS3/latest/userguide/ObjectsinRequesterPaysBuckets.html) bucket,
so [access requests have to be configured properly](https://docs.aws.amazon.com/AmazonS3/latest/userguide/ObjectsinRequesterPaysBuckets.html).

Unfortunately, Hadoop in versions available in Dataproc does not support *Requester Pays* feature. It will be available
as of Hadoop 3.3.4 ([HADOOP-14661](https://issues.apache.org/jira/browse/HADOOP-14661)).

In consequence, one need to copy the datasets to Google Storage manually before running benchmarks. The simplest
solution is to copy the data in two steps: first to a S3 bucket with *Requester Pays* disabled, then copy the data
using [Cloud Storage Transfer Service](https://cloud.google.com/storage-transfer/docs/how-to).

_________________

### Test the cluster setup
Navigate to your local copy of this repository and this benchmark directory. Then run the following steps.

#### Run simple test workload
Verify that you have the following information
  - <HOST_NAME>: Cluster master node host name
  - <PEM_FILE>: Local path to your PEM file for SSH into the master node.
  - <SSH_USER>: The username that will be used to SSH into the master node. The username is tied to the SSH key you
    have imported into the cloud. It defaults to `hadoop`.
  - <BENCHMARK_PATH>: Path where tables will be created. Make sure your credentials have read/write permission to that path.
  - <CLOUD_PROVIDER>: Currently either `gcp` or `aws`. For each storage type, different Delta properties might be added.
    
Then run a simple table write-read test: Run the following in your shell.
 
```sh
./run-benchmark.py \
    --cluster-hostname <HOSTNAME> \
    -i <PEM_FILE> \
    --ssh-user <SSH_USER> \
    --benchmark-path <BENCHMARK_PATH> \
    --cloud-provider <CLOUD_PROVIDER> \
    --benchmark test
```

If this works correctly, then you should see an output that look like this.
     
```text
>>> Benchmark script generated and uploaded

...
There is a screen on:
12001..ip-172-31-21-247	(Detached)

Files for this benchmark:
20220126-191336-test-benchmarks.jar
20220126-191336-test-cmd.sh
20220126-191336-test-out.txt
>>> Benchmark script started in a screen. Stdout piped into 20220126-191336-test-out.txt.Final report will be generated on completion in 20220126-191336-test-report.json.
```

The test workload launched in a `screen` is going to run the following: 
- Spark jobs to run a simple SQL query
- Create a Delta table in the given location 
- Read it back
    
To see whether they worked correctly, SSH into the node and check the output of 20220126-191336-test-out.txt. Once the workload terminates, the last few lines should be something like the following:
```text
RESULT:
{
  "benchmarkSpecs" : {
    "benchmarkPath" : ...,
    "benchmarkId" : "20220126-191336-test"
  },
  "queryResults" : [ {
    "name" : "sql-test",
    "durationMs" : 11075
  }, {
    "name" : "db-list-test",
    "durationMs" : 208
  }, {
    "name" : "db-create-test",
    "durationMs" : 4070
  }, {
    "name" : "db-use-test",
    "durationMs" : 41
  }, {
    "name" : "table-drop-test",
    "durationMs" : 74
  }, {
    "name" : "table-create-test",
    "durationMs" : 33812
  }, {
    "name" : "table-query-test",
    "durationMs" : 4795
  } ]
}
FILE UPLOAD: Uploaded /home/hadoop/20220126-191336-test-report.json to s3:// ...
SUCCESS
```
    
The above metrics are also written to a json file and uploaded to the given path. Please verify that both the table and report are generated in that path. 

#### Run 1GB TPC-DS
Now that you are familiar with how the framework runs the workload, you can try running the small scale TPC-DS benchmark.


1. Load data as Delta tables:
    ```bash
    ./run-benchmark.py \
        --cluster-hostname <HOSTNAME> \
        -i <PEM_FILE> \
        --ssh-user <SSH_USER> \
        --benchmark-path <BENCHMARK_PATH> \
        --cloud-provider <CLOUD_PROVIDER> \
        --benchmark tpcds-1gb-delta-load
    ```
   If you run the benchmark in GCP you should provide `--source-path <SOURCE_PATH>` parameter, where `<SOURCE_PATH>` is the location of the raw parquet input data files (see *Input data* section).
    ```bash
    ./run-benchmark.py \
        --cluster-hostname <HOSTNAME> \
        -i <PEM_FILE> \
        --ssh-user <SSH_USER> \
        --benchmark-path <BENCHMARK_PATH> \
        --source-path <SOURCE_PATH> \
        --cloud-provider gcp \
        --benchmark tpcds-1gb-delta-load
    ```

3. Run queries on Delta tables:
    ```bash
    ./run-benchmark.py \
        --cluster-hostname <HOSTNAME> \
        -i <PEM_FILE> \
        --ssh-user <SSH_USER> \
        --benchmark-path <BENCHMARK_PATH> \
        --cloud-provider <CLOUD_PROVIDER> \
        --benchmark tpcds-1gb-delta
    ```

### Run 3TB TPC-DS
Finally, you are all set up to run the full scale benchmark. Similar to the 1GB benchmark, run the following

1. Load data as Delta tables:
    ```bash
    ./run-benchmark.py \
        --cluster-hostname <HOSTNAME> \
        -i <PEM_FILE> \
        --ssh-user <SSH_USER> \
        --benchmark-path <BENCHMARK_PATH> \
        --cloud-provider <CLOUD_PROVIDER> \
        --benchmark tpcds-3tb-delta-load
    ```
   If you run the benchmark in GCP you should provide `--source-path <SOURCE_PATH>` parameter, where `<SOURCE_PATH>` is the location of the raw parquet input data files (see *Input data* section).
    ```bash
    ./run-benchmark.py \
        --cluster-hostname <HOSTNAME> \
        -i <PEM_FILE> \
        --ssh-user <SSH_USER> \
        --benchmark-path <BENCHMARK_PATH> \
        --source-path <SOURCE_PATH> \
        --cloud-provider gcp \
        --benchmark tpcds-3tb-delta-load
    ```

2. Run queries on Delta tables:
    ```bash
    ./run-benchmark.py \
        --cluster-hostname <HOSTNAME> \
        -i <PEM_FILE> \
        --ssh-user <SSH_USER> \
        --benchmark-path <BENCHMARK_PATH> \
        --cloud-provider <CLOUD_PROVIDER> \
        --benchmark tpcds-3tb-delta
    ```

Compare the results using the generated JSON files.

_________________


## Running TPC-DS Refresh benchmark

Run all steps above first to set up an EMR cluster appropriately. For Iceberg Merge on Read, we encountered S3 timeout errors. To avoid this, follow [directions from AWS EMR documentation](https://aws.amazon.com/premiumsupport/knowledge-center/emr-timeout-connection-wait/). In the paper for the refresh benchmark, we applied this configuration to Iceberg only and not to Hudi nor Delta to maintain consistency with the previous benchmark.

### Configuring S3 buckets

Create a bucket to store temporary caches:
```bash
aws s3 mb s3://lhbench-cache-$USER --region us-west-2
```

Create a bucket to store the final tables:
```bash
aws s3 mb s3://lhbench-tables-$USER --region us-west-2
```

### Benchmark commands
For all commands, set your EMR master hostname to `$CLUSTER_HOSTNAME` and set the desired scale via `$SCALE` e.g. `SCALE=1`. These commands also assume your SSH key is located at `~/.ssh/us-west-2.pem`. To customize versions of each package, modify the version definitions at the top of `run-benchmark.py`. In the paper, we tested Iceberg versions 1.1.0 and 0.14.0. After the benchmark completes, results will be copied to your local machine as with the TPC-DS benchmark.

#### Refresh benchmark: Delta CoW

```bash
$ export RUNID=$(date +%Y%m%d%H%M%S)
$ python run-benchmark.py \
    --cluster-hostname "$CLUSTER_HOSTNAME" \
    -i ~/.ssh/us-west-2.pem \
    --ssh-user hadoop \
    --scale $SCALE \
    --source-path "s3://lhbench-data/${SCALE}gb" \
    --cache-path "s3://lhbench-cache-$USER/${SCALE}gb" \
    --benchmark-path "s3://lhbench-tables-$USER/${SCALE}gb/delta/{RUNID}" \
    --db-name "deltasf${SCALE}gb${RUNID}" \
    --refresh-count 10 \
    --iterations 3 \
    --cloud-provider aws \
    --table-mode cow \
    --partition-tables true \
    --benchmark tpcds-custom-ingestion-delta
```

#### Refresh benchmark: Hudi CoW

```bash
$ export RUNID=$(date +%Y%m%d%H%M%S)
$ python run-benchmark.py \
    --cluster-hostname "$CLUSTER_HOSTNAME" \
    -i ~/.ssh/us-west-2.pem \
    --ssh-user hadoop \
    --scale $SCALE \
    --source-path "s3://lhbench-data/${SCALE}gb" \
    --cache-path "s3://lhbench-cache-$USER/${SCALE}gb" \
    --benchmark-path "s3://lhbench-tables-$USER/${SCALE}gb/hudi-cow/${RUNID}" \
    --db-name "hudicowsf${SCALE}gb${RUNID}" \
    --refresh-count 10 \
    --iterations 3 \
    --cloud-provider aws \
    --table-mode cow \
    --partition-tables true \
    --benchmark tpcds-custom-ingestion-hudi
```

#### Refresh benchmark: Hudi MoR

```bash
$ export RUNID=$(date +%Y%m%d%H%M%S)
$ python run-benchmark.py \
    --cluster-hostname "$CLUSTER_HOSTNAME" \
    -i ~/.ssh/us-west-2.pem \
    --ssh-user hadoop \
    --scale $SCALE \
    --source-path "s3://lhbench-data/${SCALE}gb" \
    --cache-path "s3://lhbench-cache-$USER/${SCALE}gb" \
    --benchmark-path "s3://lhbench-tables-$USER/${SCALE}gb/hudi-mor/${RUNID}" \
    --db-name "hudimorsf${SCALE}gb${RUNID}" \
    --refresh-count 10 \
    --iterations 3 \
    --cloud-provider aws \
    --table-mode mor \
    --partition-tables true \
    --benchmark tpcds-custom-ingestion-hudi
```

#### Refresh benchmark: Iceberg CoW

```bash
$ export RUNID=$(date +%Y%m%d%H%M%S)
$ python run-benchmark.py \
    --cluster-hostname "$CLUSTER_HOSTNAME" \
    -i ~/.ssh/us-west-2.pem \
    --ssh-user hadoop \
    --scale $SCALE \
    --source-path "s3://lhbench-data/${SCALE}gb" \
    --cache-path "s3://lhbench-cache-$USER/${SCALE}gb" \
    --benchmark-path "s3://lhbench-tables-$USER/${SCALE}gb/iceberg-cow/${RUNID}" \
    --db-name "icebergcowsf${SCALE}gb${RUNID}" \
    --refresh-count 10 \
    --iterations 3 \
    --cloud-provider aws \
    --table-mode cow \
    --partition-tables true \
    --benchmark tpcds-custom-ingestion-iceberg
```

#### Refresh benchmark: Iceberg MoR

```bash
$ export RUNID=$(date +%Y%m%d%H%M%S)
$ python run-benchmark.py \
    --cluster-hostname "$CLUSTER_HOSTNAME" \
    -i ~/.ssh/us-west-2.pem \
    --ssh-user hadoop \
    --scale $SCALE \
    --source-path "s3://lhbench-data/${SCALE}gb" \
    --cache-path "s3://lhbench-cache-$USER/${SCALE}gb" \
    --benchmark-path "s3://lhbench-tables-$USER/${SCALE}gb/iceberg-mor/${RUNID}" \
    --db-name "icebergmorsf${SCALE}gb${RUNID}" \
    --refresh-count 10 \
    --iterations 3 \
    --cloud-provider aws \
    --table-mode mor \
    --partition-tables true \
    --benchmark tpcds-custom-ingestion-iceberg
```

_________________

## Running refresh microbenchmark

### Configuring S3 bucket

Create a bucket to store tables:
```bash
$ aws s3 mb s3://lhbench-tables-microbenchmark-$USER --region us-west-2
```

### Benchmark commands

For all commands, set your EMR master hostname to `$CLUSTER_HOSTNAME` and set the desired scale via `$SCALE` e.g. `SCALE=1`. These commands also assume your SSH key is located at `~/.ssh/us-west-2.pem`. To customize versions of each package, modify the version definitions at the top of `run-benchmark.py`.

For all microbencmark commands, set the percentage of updated rows with the `$PCT` environment variable. For example, to choose 0.1%,run `export PCT=0.001`.

#### Refresh microbenchark: Delta

```bash
$ export RUNID=$(date +%Y%m%d%H%M%S)
$ python run-benchmark.py \
    --cluster-hostname "$CLUSTER_HOSTNAME" \
    -i ~/.ssh/us-west-2.pem \
    --ssh-user hadoop \
    --cloud-provider aws \
    --benchmark-path "s3://lhbench-tables-microbenchmark-$USER/delta/${RUNID}" \
    --benchmark "merge-micro-${SCALE}gb-delta" \
    --db-name "deltamicrosf${SCALE}gb${RUNID}" \
    --partition-table false \
    --source-percent $PCT
```

#### Refresh microbenchark: Hudi CoW

```bash
$ export RUNID=$(date +%Y%m%d%H%M%S)
$ python run-benchmark.py \
    --cluster-hostname "$CLUSTER_HOSTNAME" \
    -i ~/.ssh/us-west-2.pem \
    --ssh-user hadoop \
    --cloud-provider aws \
    --benchmark-path "s3://lhbench-tables-microbenchmark-$USER/hudi-cow/${RUNID}" \
    --benchmark "merge-micro-${SCALE}gb-hudi-cow" \
    --db-name "hudicowmicrosf${SCALE}gb${RUNID}" \
    --partition-table false \
    --source-percent $PCT
```

#### Refresh microbenchark: Hudi MoR

```bash
$ export RUNID=$(date +%Y%m%d%H%M%S)
$ python run-benchmark.py \
    --cluster-hostname "$CLUSTER_HOSTNAME" \
    -i ~/.ssh/us-west-2.pem \
    --ssh-user hadoop \
    --cloud-provider aws \
    --benchmark-path "s3://lhbench-tables-microbenchmark-$USER/hudi-mor/${RUNID}" \
    --benchmark "merge-micro-${SCALE}gb-hudi-mor" \
    --db-name "hudimormicrosf${SCALE}gb${RUNID}" \
    --partition-table false \
    --source-percent $PCT
```

#### Refresh microbenchark: Iceberg CoW

```bash
$ export RUNID=$(date +%Y%m%d%H%M%S)
$ python run-benchmark.py \
    --cluster-hostname "$CLUSTER_HOSTNAME" \
    -i ~/.ssh/us-west-2.pem \
    --ssh-user hadoop \
    --cloud-provider aws \
    --benchmark-path "s3://lhbench-tables-microbenchmark-$USER/iceberg-cow/${RUNID}" \
    --benchmark "merge-micro-${SCALE}gb-iceberg" \
    --db-name "icebergcowmicrosf${SCALE}gb${RUNID}" \
    --partition-table false \
    --source-percent $PCT
```

#### Refresh microbenchark: Iceberg MoR

```bash
$ export RUNID=$(date +%Y%m%d%H%M%S)
$ python run-benchmark.py \
    --cluster-hostname "$CLUSTER_HOSTNAME" \
    -i ~/.ssh/us-west-2.pem \
    --ssh-user hadoop \
    --cloud-provider aws \
    --benchmark-path "s3://lhbench-tables-microbenchmark-$USER/iceberg-mor/${RUNID}" \
    --benchmark "merge-micro-${SCALE}gb-iceberg-mor" \
    --db-name "icebergmormicrosf${SCALE}gb${RUNID}" \
    --partition-table false \
    --source-percent $PCT
```

_________________

## Internals of the framework

Structure of this framework's code
- `build.sbt`, `project/`, `src/` form the SBT project which contains the Scala code that define the benchmark workload.
    - `Benchmark.scala` is the basic interface, and `TestBenchmark.scala` is a sample implementation.
- `run-benchmark.py` contains the specification of the benchmarks defined by name (e.g. `tpcds-3tb-delta`). Each benchmark specification is defined by the following: 
    - Fully qualified name of the main Scala class to be started.
    - Command line argument for the main function.
    - Additional Maven artifact to load (example `io.delta:delta-core_2.12:1.0.0`).
    - Spark configurations to use.
- `scripts` has the core python scripts that are called by `run-benchmark.py`

The script `run-benchmark.py` does the following:
- Compile the Scala code into a uber jar.
- Upload it to the given hostname.
- Using ssh to the hostname, it will launch a screen and start the main class with spark-submit.
