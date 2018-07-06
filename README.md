# Scala Spark project
A Scala - Spark based project to experiment with map-reduce algorithms on big data graph shaped. The dependencies and assembly for this project (like Apache Spark) are done using sbt.

## How to deploy MapReduce jobs on Google Cloud Platform clusters

## Setup cluster
+ In Dataproc setup the configuration of the cluster (better in zones: europe-west-1/2/__3__) and run it
+ Download and install locally gcloud CLI tool:
    + set up the project_id property `gcloud config set project p_id` with p_id shown in Web UI
+ `hdfs dfs -mkdir -p hdfs://spark-cluster-m/user/battilanap`

## Deploy project
+ Build your ScalaSpark project in .jar file (specifying `% 'provided'` in _build.sbt_ Spark dependencies and then using SBT `assembly` command)
+ Copy also the graph data
+ Run the Spark hob in one of following way: 
    + Copy the jar to the cluster `gcloud compute scp target/scala-2.11/scalasparkproject_2.11-0.1.jar battilanap@spark-cluster-m:/home/battilanap --zone europe-west3-a`
    + SSH into the master node and run:
        + `gcloud compute ssh battilanap@spark-cluster-m --zone europe-west3-a` 
        + `spark-submit scalasparkproject_2.11-0.1.jar --options`
    + Submit a Dataproc job (from the directory of your .jar):
        + `gcloud dataproc jobs submit spark --cluster spark-cluster --region europe-west3 --jar scalasparkproject_2.11-0.1.jar -- --debug` 
+ Inspect or retrieve output files from the HDFS (while SSHed in master) `hadoop fs -ls /user/battilanap`

## Monitor execution
It can be done using the Web UIs of the GCP cluster. First setup SSH tunnelling:
+ `gcloud compute ssh battilanap@spark-cluster-m --zone europe-west3-a -- -D 1080 -N`
+ `/usr/bin/google-chrome --proxy-server="socks5://localhost:1080" --host-resolver-rules="MAP * 0.0.0.0 , EXCLUDE localhost" --user-data-dir=/tmp/spark-cluster-m`
Then inside the Chrome browser enter URL:
+ `http://spark-cluster-m:8088` for YARN 
+ `http://spark-cluster-m:9870` for Hadoop 
+ `http://spark-cluster-m:18080` for Spark history server

### Logs
YARN log aggregation
+ `yarn.log-aggregation-enable` and then `yarn logs -applicationId <app ID>`

gcloud dataproc jobs submit log flags
Verbosity of log4j is controlled by /etc/spark/conf/log4j.properties or overridden by the flag `--driver-log-levels root=FATAL,com.example=INF ...`

GCLOUD wide flags 
+ --verbosity=V   V::={debug, info, warning, error, none}
+ --user-output-enabled

## Spark configuration
[Blog](https://spoddutur.github.io/spark-notes/distribution_of_executors_cores_and_memory_for_spark_application.html) 
and official [doc](http://spark.apache.org/docs/latest/hardware-provisioning.html) 
with tips for configuring the three fundamental Yarn/Spark parameters when launching our Spark job
+ --num-executors
+ --executor-cores
+ --executor-memory

# Tested algorithms performance
+ soc-Epinions1
    + local[8]
        + triangles_count_V4: 643.0 seconds = 10,71 min
    + 1m4s (2vCPU/7.5GB RAM)
        + triangles_count_V1: 63 min
    + 1m5s (16vCPU/16GB RAM)
        + f_recs: 23 seconds
        + triangles_count_V4: 577.0 seconds = 9,6 min
        + triangles_count_V4 (repartition 85): 68.0 seconds 
        + triangles_count_V4 (repartition 148): 55.0 seconds
        + triangles_count_V4 (repartition 148, HDFS loading): 53.0 seconds 
      