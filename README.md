A spark program to read and write from ADLS
-------------------------------------------

Here is how you can use livy to submit jobs to process data from ADLS

The flow is:

spark job via rest api --> livy --> spark-dispatcher --> mesos --> spark job (read/write from adls)


After ssh-tunnel

with credentials hard-coded:

curl -v -H "Content-Type: application/json" -X POST -d '{ "file": "https://<url>/adls-spark-examples-0.0.1-SNAPSHOT-jar-with-dependencies.jar", "className":"TestSparkWordCount" }' 'http://localhost/service/livy-spark2/batches'

with credentials as parameters:

curl -v -H "Content-Type: application/json" -X POST -d '{ "file": "https://myjars.blob.core.windows.net/myjars/adls-spark-examples-0.0.1-SNAPSHOT-jar-with-dependencies.jar", "className":"TestHiveTable", "args": ["val for credentail", "val for clientid", "https://login.microsoftonline.com/blah-blah/oauth2/token"] }' 'http://localhost/service/livy-spark2/batches'

(ADLS jars in src/main/resources as maven central do not have the jars for hadoop 2.x)
