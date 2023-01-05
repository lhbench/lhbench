#!/usr/bin/env python3

#
# Copyright (2021) The Delta Lake Project Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import argparse
from scripts.benchmarks import *

spark_version = "3.3.0"
delta_version = "2.2.0"
iceberg_version = "1.1.0"
hudi_version = "0.12.0"

# Benchmark name to their specifications. See the imported benchmarks.py for details of benchmark.

benchmarks = {
    "test":
        DeltaBenchmarkSpec(
            delta_version=delta_version,
            benchmark_main_class="benchmark.TestBenchmark",
            main_class_args=["--test-param", "value"],
        ),

    # TPC-DS data load
    "tpcds-1gb-delta-load": DeltaTPCDSDataLoadSpec(delta_version=delta_version, scale_in_gb=1),
    "tpcds-3tb-delta-load": DeltaTPCDSDataLoadSpec(delta_version=delta_version, scale_in_gb=3000),
    "tpcds-1gb-parquet-load": ParquetTPCDSDataLoadSpec(scale_in_gb=1),
    "tpcds-3tb-parquet-load": ParquetTPCDSDataLoadSpec(scale_in_gb=3000),

    # TPC-DS benchmark
    "tpcds-1gb-delta": DeltaTPCDSBenchmarkSpec(delta_version=delta_version, scale_in_gb=1),
    "tpcds-3tb-delta": DeltaTPCDSBenchmarkSpec(delta_version=delta_version, scale_in_gb=3000),
    "tpcds-1gb-parquet": ParquetTPCDSBenchmarkSpec(scale_in_gb=1),
    "tpcds-3tb-parquet": ParquetTPCDSBenchmarkSpec(scale_in_gb=3000),

    "tpcds-1gb-iceberg-load": IcebergTPCDSDataLoadSpec(iceberg_version=iceberg_version, scale_in_gb=1),
    "tpcds-1gb-hudi-load": HudiTPCDSDataLoadSpec(hudi_version=hudi_version, scale_in_gb=1),

    "tpcds-3tb-iceberg-load": IcebergTPCDSDataLoadSpec(iceberg_version=iceberg_version, scale_in_gb=3000),
    "tpcds-3tb-hudi-load": HudiTPCDSDataLoadSpec(hudi_version=hudi_version, scale_in_gb=3000),

    "tpcds-1gb-iceberg": IcebergTPCDSBenchmarkSpec(iceberg_version=iceberg_version, scale_in_gb=1),
    "tpcds-1gb-hudi": HudiTPCDSBenchmarkSpec(hudi_version=hudi_version, scale_in_gb=1),

    "tpcds-3tb-iceberg": IcebergTPCDSBenchmarkSpec(iceberg_version=iceberg_version, scale_in_gb=3000),
    "tpcds-3tb-hudi": HudiTPCDSBenchmarkSpec(hudi_version=hudi_version, scale_in_gb=3000),
    
    # Incremental benchmark
    "tpcds-custom-ingestion-delta": DeltaIncrementalTPCDSBenchmarkSpec(delta_version=delta_version),
    "tpcds-custom-ingestion-iceberg": IcebergIncrementalTPCDSBenchmarkSpec(iceberg_version=iceberg_version),
    "tpcds-custom-ingestion-hudi": HudiIncrementalTPCDSBenchmarkSpec(hudi_version=hudi_version),
}

for size in [1, 10, 100]:
    benchmarks.update({
        f"merge-micro-{size}gb-delta": DeltaMergeMicroBenchmarkSpec(
            delta_version=delta_version, size_in_gb=size),

        f"merge-micro-{size}gb-iceberg": IcebergMergeMicroBenchmarkSpec(
            iceberg_version=iceberg_version, size_in_gb=size, merge_on_read=False),

        f"merge-micro-{size}gb-iceberg-mor": IcebergMergeMicroBenchmarkSpec(
            iceberg_version=iceberg_version, size_in_gb=size, merge_on_read=True),

        f"merge-micro-{size}gb-hudi-cow": HudiMergeMicroBenchmarkSpec(
            hudi_version=hudi_version, size_in_gb=size, merge_on_read=False),
        f"merge-micro-{size}gb-hudi-mor": HudiMergeMicroBenchmarkSpec(
            hudi_version=hudi_version, size_in_gb=size, merge_on_read=True),
    })



file_count_benchmark_params = [
    (1, 0, 1, "s3://devrel-delta-datasets/tpcds-2.13/tpcds_sf1_parquet/store_sales/"),
    (50, 0.5, 3000, "s3://devrel-delta-datasets/tpcds-2.13/tpcds_sf3000_parquet/store_sales/"),
    (200, 0.5, 3000, "s3://devrel-delta-datasets/tpcds-2.13/tpcds_sf3000_parquet/store_sales/"),
    (50, 3.5, 30000, "s3://databricks-tdas/cidr-benchmarks/data/tpcds-2.13/tpcds_sf30000_parquet_nostats/store_sales/")
]


delta_log_store_classes = {
    "aws": "spark.delta.logStore.class=org.apache.spark.sql.delta.storage.S3SingleDriverLogStore",
    "gcp": "spark.delta.logStore.gs.impl=io.delta.storage.GCSLogStore",
}

if __name__ == "__main__":
    """
    Run benchmark on a cluster using ssh.

    Example usage:

    ./run-benchmark.py --cluster-hostname <hostname> -i <pem file> --ssh-user <ssh user> --cloud-provider <cloud provider> --benchmark test

    """


# ========== FileCountBenchmark on sorted custom tables ==========
for file_count_in_k in [0.1, 1, 10, 50, 100, 200, 500, 1000]:
    benchmarks.update({

        f"{file_count_in_k}k-files-sorted-delta-load": DeltaFileCountBenchmarkSpec(
            delta_version=delta_version,
            num_files=int(file_count_in_k * 1000),
            main_class_args=[
                "--sorted",
                "--loadMode", "reload",
            ]
        ),

        f"{file_count_in_k}k-files-sorted-delta-query": DeltaFileCountBenchmarkSpec(
            delta_version=delta_version,
            num_files=int(file_count_in_k * 1000),
            main_class_args=[
                "--sorted",
                "--loadMode", "noload",
            ]),

        f"{file_count_in_k}k-files-sorted-iceberg-load": IcebergFileCountBenchmarkSpec(
            iceberg_version=iceberg_version,
            num_files=int(file_count_in_k * 1000),
            main_class_args=[
                "--sorted",
                "--loadMode", "reload",
            ]
        ),

        f"{file_count_in_k}k-files-sorted-iceberg-query": IcebergFileCountBenchmarkSpec(
            iceberg_version=iceberg_version,
            num_files=int(file_count_in_k * 1000),
            main_class_args=[
                "--sorted",
                "--loadMode", "noload",
            ]),
    })


def parse_args():
    # Parse cmd line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--benchmark", "-b",
        required=True,
        help="Run the given benchmark. See this " +
             "python file for the list of predefined benchmark names and definitions.")
    parser.add_argument(
        "--cluster-hostname",
        required=True,
        help="Hostname or public IP of the cluster driver")
    parser.add_argument(
        "--ssh-id-file", "-i",
        required=True,
        help="SSH identity file")
    parser.add_argument(
        "--spark-conf",
        action="append",
        help="Run benchmark with given spark conf. Use separate --spark-conf for multiple confs.")
    parser.add_argument(
        "--resume-benchmark",
        help="Resume waiting for the given running benchmark.")
    parser.add_argument(
        "--use-local-delta-dir",
        help="Local path to delta repository which will be used for running the benchmark " +
             "instead of the version specified in the specification. Make sure that new delta" +
             " version is compatible with version in the spec.")
    parser.add_argument(
        "--cloud-provider",
        choices=delta_log_store_classes.keys(),
        help="Cloud where the benchmark will be executed.")
    parser.add_argument(
        "--ssh-user",
        default="hadoop",
        help="The user which is used to communicate with the master via SSH.")

    parsed_args, parsed_passthru_args = parser.parse_known_args()
    return parsed_args, parsed_passthru_args


def run_single_benchmark(benchmark_name, benchmark_spec, other_args):
    benchmark_spec.append_spark_confs(other_args.spark_conf)
    benchmark_spec.append_spark_conf(delta_log_store_classes.get(other_args.cloud_provider))
    benchmark_spec.append_main_class_args(passthru_args)
    print("------")
    print("Benchmark spec to run:\n" + str(vars(benchmark_spec)))
    print("------")

    benchmark = Benchmark(benchmark_name, benchmark_spec,
                          use_spark_shell=True, local_delta_dir=other_args.use_local_delta_dir)
    benchmark_dir = os.path.dirname(os.path.abspath(__file__))
    with WorkingDirectory(benchmark_dir):
        benchmark.run(other_args.cluster_hostname, other_args.ssh_id_file, other_args.ssh_user)


if __name__ == "__main__":
    """
    Run benchmark on a cluster using ssh.

    Example usage:

    ./run-benchmark.py --cluster-hostname <hostname> -i <pem file> --ssh-user <ssh user> --cloud-provider <cloud provider> --benchmark test

    """
    args, passthru_args = parse_args()

    if args.resume_benchmark is not None:
        Benchmark.wait_for_completion(
            args.cluster_hostname, args.ssh_id_file, args.resume_benchmark, args.ssh_user)
        exit(0)

    benchmark_names = args.benchmark.split(",")
    for benchmark_name in benchmark_names:
        # Create and run the benchmark
        if benchmark_name in benchmarks:
            run_single_benchmark(benchmark_name, benchmarks[benchmark_name], args)
        else:
            raise Exception("Could not find benchmark spec for '" + benchmark_name + "'." +
                            "Must provide one of the predefined benchmark names:\n" +
                            "\n".join(benchmarks.keys()) +
                            "\nSee this python file for more details.")
