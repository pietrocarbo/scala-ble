# Scala Spark project

A Scala based project to speriment map-reduce algorithms on big data (graph shaped). The dependecies of the project (like Apache Spark) are resolved using sbt.


## Google Cloud Platform deploy
+ Build your ScalaSpark project in .jar file (using SBT `package`)
+ In Dataproc setup the configuration of the cluster (better in zones: europe-west-1/2/__3__) and run it
+ Download and install locally gcloud CLI tool:
    + set up the project_id property `gcloud config set project p_id` with p_id shown in Web UI
+ Copy the jar `gcloud compute scp target/scala-2.11/scalasparkproject_2.11-0.1.jar battilanap@spark-cluster-m:/home/battilanap
`
+ SSH into the master node with `gcloud compute ssh battilanap@spark-cluster-m`
+ Submit the Spark job `spark-submit scalasparkproject_2.11-0.1.jar --options`