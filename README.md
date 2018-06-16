# Scala Spark project
A Scala based project to speriment map-reduce algorithms on big data (graph shaped). The dependecies of the project (like Apache Spark) are resolved using sbt.

## Google Cloud Platform

### Setup cluster
+ In Dataproc setup the configuration of the cluster (better in zones: europe-west-1/2/__3__) and run it
+ Download and install locally gcloud CLI tool:
    + set up the project_id property `gcloud config set project p_id` with p_id shown in Web UI

### Deploy project
+ Build your ScalaSpark project in .jar file (using SBT `package`)
+ Copy also the graph data
+ Run the Spark hob in one of following way: 
    + Copy the jar to the cluster `gcloud compute scp target/scala-2.11/scalasparkproject_2.11-0.1.jar battilanap@spark-cluster-m:/home/battilanap`
    + SSH into the master node and run:
        + `gcloud compute ssh battilanap@spark-cluster-m` 
        + `spark-submit scalasparkproject_2.11-0.1.jar --options`
    + Submit a Dataproc job (from the directory of your .jar):
        + `gcloud dataproc jobs submit spark --cluster spark-cluster --region europe-west3 --jar scalasparkproject_2.11-0.1.jar -- --debug `
            + 
+ Inspect or retrieve output files from the HDFS (while SSHed in master) `hadoop fs -ls /user/battilanap`

### Monitor execution
It can be done using the Web UIs of the GCP cluster. First setup SSH tunnelling:
+ `gcloud compute ssh battilanap@spark-cluster-m --zone europe-west3-a -- -D 1080 -N`
+ `/usr/bin/google-chrome --proxy-server="socks5://localhost:1080" --host-resolver-rules="MAP * 0.0.0.0 , EXCLUDE localhost" --user-data-dir=/tmp/spark-cluster-m`
Then inside the Chrome browser enter URL:
+ `http://spark-cluster-m:8088` for YARN 
+ `http://spark-cluster-m:9870` for Hadoop 

### gcloud dataproc jobs submit log flags
Verbosity of log4j is controlled by /etc/spark/conf/log4j.properties or overridden by the flag `--driver-log-levels root=FATAL,com.example=INF ...`

## GCLOUD wide flags 
+ --verbosity=V   V::={debug, info, warning, error, none}
+ --user-output-enabled