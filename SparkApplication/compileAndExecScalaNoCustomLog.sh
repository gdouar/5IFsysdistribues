sbt package && /usr/local/spark/bin/spark-submit --class "SimpleApp" sbt  --master local[4] target/scala-2.10/simple-project_2.10-1.0.jar