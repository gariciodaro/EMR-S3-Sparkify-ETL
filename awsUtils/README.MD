# Steps to execute pyspark on ERM

+ Create an s3 bucket with ```createAwsBucket.py```.
+ Create an EMR cluster with Spark with ```createEMR.py```
+ Make sure your ip has permission to SSH to cluster on **Security groups for Master**. Security groups of master node->inbound rules, new rule with SSH on MyIp.
+ Make sure your keypair.pem file has the correct permissions, ```chmod 400 ./keypair.pem```
+ check that you can now SSH to the EMR cluster.
+ Copy the pyspark script into the EMR.
+ ```scp -i ~/keypair.pem /pyspark_script.py hadoop@xxxxxxx.compute.amazonaws.com:/home/hadoop/```
+ On the cluster where  spark-submit is located ```which spark-submit```.
+ Execute script ```/usr/bin/spark-submit --master yarn ./etl.py```

+ If you want to monitor the cluster, Open an SSH Tunnel to the Amazon EMR Master Node on terminal
+ configure FoxyProxy.


