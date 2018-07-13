# Scala Spark Project
A Scala (v2.11) - Spark (v2.3) based project to experiment with map-reduce algorithms on big data graph shaped. 

The dependencies (only Apache Spark core) linking and building for this project is done via `sbt`.

To avoid dealing with duplicate and/or conflicts between libraries, before
launching the command `sbt assembly` to create the __.jar__ artifact,
add the `provided` option to the `org.apache.spark.spark-core` dependency in the `build.sbt` file. 

Then you can launch the application using the `spark-submit <jarFile> --appArguments` command.

## Command-line arguments
Usage: `ScalaSparkProject.jar [--debug] [--local N_CORES] [--partitions N_PART] [--outputs] [--trasform MODE] [--dpr N_DIGITS] [--friends-rec N_RECS] [--triangles] --input GRAPHFILE`

The last four optional arguments are the main program functions also called _jobs_. If no job is specified as a command-line argument, __ALL__ jobs gets executed.\
You can run the application locally and control the number of cores used using the `--local` option.\
Giving the argument `--outputs` the program will save the resulting RDDs in a folder `./outputs/jobName`.

__OPTIONS__ 
* `--local N_CORES` where _N_CORES_ must be an Integer indicating the number of cores to use. __Only__ when launching the application locally.
* `--partitions N_PARTS` where _N_PARTS_ must be an Integer > 0 indicating the number of partitions for the main edges RDD. Recommended value is `#cores*#executors*1.75` (__powerful__ flag to tune parallelism, use with caution).
* `--trasform MODE` where _MODE_ must be an Integer between 0 and 2 to indicate the kind of, per-vertex, edges list to get (`0`: outgoing links, `1`: ingoing links, `2`: both).
* `--dpr N_DIGITS` where _N_DIGITS_ must be an Integer between 1 and 20 to shift down the decimal digits for the tolerance value (e.g. `--dpr 4` => toll = 1e-4).
* `--friends-rec N_RECS` where _N_RECS_ must be an Integer between 0 and 50 specifying the number of the, per-user/vertex, friends recommendations wanted (`0`: to emit all recommendations).


## How to deploy Spark jobs on Google Cloud Platform clusters

## Configure cluster
1. Create a project via the [Cloud Console](https://console.cloud.google.com)
1. Download the Cloud SDK from the official source [here](https://cloud.google.com/sdk/) and install it on your system
1. Use the command `gcloud config set project <projectID>` (as explained [here](https://cloud.google.com/sdk/gcloud/reference/config/set)) 
where `projectID` is the ID associated with project created at point 1.
1. Go to the [webpage](https://console.cloud.google.com/dataproc) of Dataproc and create a cluster giving it the name _spark-cluster_ (the nearest region is _europe-west3-a_ i.e. Frankfurt)
1. You can access the master node of your cluster with the command `gcloud compute ssh <yourGCPusername>@spark-cluster-m --zone europe-west3-a` 
1. Run the command `hdfs dfs -mkdir -p hdfs://spark-cluster-m/user/<yourGCPusername>` to create a folder in the HDFS filesystem

## Deploy project
+ Build your ScalaSpark project into a .jar file (specifying the _spark-core_ dependency as `% 'provided'` in the __build.sbt__ file and then the `sbt assembly` command)
+ Copy to the cluster master node:
    + the application .jar with the command `gcloud compute scp <path/of/app.jar> <yourGCPusername>@spark-cluster-m:/home/<yourGCPusername> --zone europe-west3-a`
    + the graph dataset to operate on with the command `gcloud compute scp <path/of/graphfile.txt> <yourGCPusername>@spark-cluster-m:/home/<yourGCPusername> --zone europe-west3-a`
+ Upload the input graph file to HDFS:
    + SSH into the master node (point 5. of the previous section) and run the command `hdfs dfs -put <graphfile.txt>`
+ Run the Spark application on the master node via YARN using one of the two following ways: 
    + SSH into the master node (point 5. of the previous section) and run the command `spark-submit <app.jar> --appArguments`
    + Submit a Dataproc job:
        + via the GCP Web Console in the Dataproc Job [section](https://console.cloud.google.com/dataproc/jobs)
        + from the directory of your .jar `gcloud dataproc jobs submit spark --cluster spark-cluster --region europe-west3 --jar <app.jar> -- --appArguments` 

## Monitor execution
The status of the applications running on your cluster can be monitored using several web pages. 

First setup SSH tunnelling to the master node of your cluster with the commands:
+ `gcloud compute ssh <yourGCPusername>@spark-cluster-m --zone europe-west3-a -- -D 1080 -N`
+ `/usr/bin/google-chrome --proxy-server="socks5://localhost:1080" --host-resolver-rules="MAP * 0.0.0.0 , EXCLUDE localhost" --user-data-dir=/tmp/spark-cluster-m`

Then inside the Chrome browser enter URL:spark.dynamicAllocation.enabled
+ `http://spark-cluster-m:8088` for YARN Manager   
+ `http://spark-cluster-m:9870` for Hadoop 
+ `http://spark-cluster-m:18080` for Spark History server

### Logs verbosity
The level of the log verbosity is defined in `/etc/spark/conf/log4j.properties` files and can be overridden by the flag `--driver-log-levels root=FATAL,com.example=INFO`

## Output files
+ Inspect and/or retrieve output files from the HDFS filesytem in two ways:
    + using the gui of the Hadoop [webpage](http://spark-cluster-m:9870/explorer.html#/)
    + using the CLI Hadoop commands (while SSHed into the master node): `hadoop dfs -ls ` , etc..

## Executors and memory configuration
When running a Spark application we have to consider three fundamental (Spark/YARN) parameters to get the best performance: 
+ `--num-executors`
+ `--executor-cores`
+ `--executor-memory`

The official Spark [documentation](http://spark.apache.org/docs/latest/hardware-provisioning.html),
[this](https://spoddutur.github.io/spark-notes/distribution_of_executors_cores_and_memory_for_spark_application.html) and [this](http://site.clairvoyantsoft.com/understanding-resource-allocation-configurations-spark-application/)
blog posts give some hints for an optimal configuration of these parameters.  

## Algorithms performance
+ Using the dataset soc-Epinions1.txt [75K nodes, 508K edges] (available from SNAP [here](https://snap.stanford.edu/data/soc-Epinions1.html))
    + Locally with `local[8]`(JVM memory -Xms10g -Xmx14g) :
        + trasform(2): 2.0 seconds
        + dynamicPageRank(8): 2.0 seconds
        + friends_recommendations(0): 22.0 seconds
        + triangles_count_V4: 643.0 seconds = 10,71 min
    + Spark cluster of `1 master - 4 slaves` (each with 2 CPU / 7.5GB RAM):
        + triangles_count_V1: 63 min
    + Spark cluster of `1 master - 5 slaves` (each with 16 CPU / 16GB RAM):
        + friends_recommendations: 23 seconds
        + triangles_count_V4: 577.0 seconds = 9,6 min
        + triangles_count_V4 (repartition 85): 68.0 seconds 
        + triangles_count_V4 (repartition 148): 55.0 seconds
        + __triangles_count_V4 (repartition 148, HDFS input parallel loading): 53.0 seconds__
            + launch command `spark-submit --conf spark.dynamicAllocation.enabled=false --num-executors 17 --executor-cores 5 --executor-memory 19G ScalaSparkProject-assembly-0.1.jar --debug --outputs --partitions 148 --input soc-Epinions1.txt` 
            + trasform(2): 12 seconds
            + dynamicPageRank(8): 5 seconds
            + friends_recommendations(0): 19 seconds

+ Using the dataset soc-LiveJournal1.txt [4.8M nodes, 68M edges] (available from SNAP [here](https://snap.stanford.edu/data/soc-LiveJournal1.html))
    + Spark cluster of `1 master - 5 slaves` (each with 16 CPU / 16GB RAM):
        + trasform(2): 38 seconds
        + dynamicPageRank(8): 14 seconds
        + friends_recommendations(0): 806 seconds = 13.3 minutes
        + triangles_count_V4: NA (due to OOM errors)