# aerospike-migrations

1)  Objective
The following example illustrates how Spark can be deployed to transform data between formats and independent sources and/or targets. Specifically: 

Extract existing relational data (a PostgreSQL-compatible database is demonstrated in this example), transform it to CSV or JSON formatted data, then push the transformed data to an S3 bucket for consumption by ChaosSeach 
Read parquet formatted data files from an S3 bucket, transform it to CSV or JSON formatted data, then push the transformed data to an S3 bucket for consumption by ChaosSeach

Scala notebooks only are presented.
2)  System Configuration
Spark:
Databricks Enterprise Edition:
7.6 (includes Apache Spark 3.0.1, Scala 2.12)
AWS, us-east-1d
1 node - m5d.large

PostgreSQL:
Yugabyte Enterprise 2.7.0:
GCP
Three n1-standard-8 nodes
Replication Factor: 3 (RF=3)
1 zone (us-central1), and 3 availability zones (a, b, c)
Encryption in-transit enabled
OpenSSL version 2.8.3
Note: Yugabyte access port is 5433 (vs well-known 5432)
3)  SSL
Encryption-in-transit (EIT)  is often enabled to secure the source database. This example also illustrates how to configure the client java JDBC PostgreSQL driver to access EIT-enabled sources. 

To make the certificates compatible with Java in Databricks, the client certificate and key file must be converted to DER format:

1)  Client certificate (client_cert.crt):

bash:
```
openssl x509 -in client_cert.crt -out client_cert.crt.der -outform der
```

2)  Client key files (key_file.key):

bash:
```
openssl pkcs8 -topk8 -outform der -in key_file.key -out key_file.key.pk8 -nocrypt
```

3)  Note:  The root certificate does not need conversion (root.crt).

4)  Install databricks-cli (if not installed):

bash:
```
pip install databricks-cli
```

5)  Create a certificate folder in Databricks DBFS and copy the following files to that folder (/dbfs/<path-to-certs>/) (refer to the Databricks documentation for details):

client_cert.crt.der
key_file.key.pk8
root.crt

6)  In the Databricks Scala notebook, set the following JDBC Driver options in either of the following ways:

Scala Notebook:
```
val jdbcDF = spark.read
  .format("jdbc")
  .option("url", "jdbc:postgresql://<ip or qualified url>:5433/<database>") 
  .option("dbtable", "<schema>.<table>")
  .option("user", "<username>")
  .option("ssl", "true")
  .option("sslmode", "verify-ca")
  .option("sslcert", "/dbfs/<path-to-certs>/client_cert.crt.der" )
  .option("sslkey", "/dbfs/<path-to-certs>/key_file.key.pk8" )
  .option("sslrootcert", "/dbfs/<path-to-certs>/root.crt")
  ```
or

Scala Notebook:
```
val jdbcDF = spark.read
  .format("jdbc")
  .option("url", "jdbc:postgresql://<ip or qualified url>:5433/<database>") 
  .option("dbtable", "<schema>.<table>")
  .option("user", "<username>")
  .option("ssl", "true")
  .option("sslmode", "require")
  .option("sslcert", "/dbfs/<path-to-certs>/client_cert.crt.der" )
  .option("sslkey", "/dbfs/<path-to-certs>/key_file.key.pk8" )
```

If SSL is enabled on the source database, substitute either of the above read statements for that shown in the Section 5 Scala Notebook, below.
4)  Mount an S3 bucket in Databricks
1)  Create an AWS S3 API User

From AWS IAM console => Users

=> Add users

Create username, Select AWS access type: check Access Key - Programmatic access

=> Next Permissions


Select Attach existing policies directly => AmazonS3FullAccess

=> Next: Tags

Assign tags (for reference)

=> Next: Review










=> Create user

Access key ID and Secret access key must be added to a Databricks Secret Scope detailed in step 3), below. Copy and save...

=> Close

2)  Create a Databricks Access Token

From the workspace, select Settings => User Settings


From the User Setting Page, select the Access Tokens tab -> Generate New Token.  Enter a comment/name.

=> Generate
Copy the token presented as it will no longer be visible/retrievable once Done is selected.  This token will be required for CLI access, described below. Copy and save for later use.
=> Done

3)  Create a Databricks Secrets Scope for AWS credentials

bash:
```
databricks configure --token
<paste token>
databricks secrets create-scope --scope aws
databricks secrets list-scopes
<view output>
databricks secrets put --scope aws --key aws-access-key
<paste access-key from AWS S3 API User, created above>
databricks secrets put --scope aws --key aws-secret-key
<paste secret-key from AWS S3 API User, created above>
databricks secrets list --scope aws
<view output>
```

4) Mount the S3 bucket in Databricks

Scala Notebook:
```  
val AccessKey = dbutils.secrets.get(scope = "aws", key = "aws-access-key")
// Encode the Secret Key as that can contain "/"
val SecretKey = dbutils.secrets.get(scope = "aws", key = "aws-secret-key")
val EncodedSecretKey = SecretKey.replace("/", "%2F")
val AwsBucketName = "jj-chaos"
val MountName = "s3-jj-chaos"

dbutils.fs.mount(s"s3a://$AccessKey:$EncodedSecretKey@$AwsBucketName", s"/mnt/$MountName")
display(dbutils.fs.ls(s"/mnt/$MountName"))
```

Results:

The mount will remain in-scope for the lifetime of the workspace (until deleted).

5)  Extract Existing PostgreSQL Data and Transform to Parquet/JSON/CSV Formatted Data
1)  Load Data

Sample data (CSV):
IP,Time,URL,Status
10.128.2.1,29-Nov-2017 06:58:55,GET /login.php HTTP/1.1,200
10.128.2.1,29-Nov-2017 06:59:02,POST /process.php HTTP/1.1,302
10.128.2.1,29-Nov-2017 06:59:03,GET /home.php HTTP/1.1,200
10.131.2.1,29-Nov-2017 06:59:04,GET /js/vendor/moment.min.js HTTP/1.1,200
10.130.2.1,29-Nov-2017 06:59:06,GET /bootstrap-3.3.7/js/bootstrap.js HTTP/1.1,200

<Download weblog5k.csv here>

SQL shell:
# create database logs;
# \c logs;
# create schema server1;
# set schema 'server1';
# create table weblog(ip text, datetime timestamp, url text, status int);
# \copy weblog from weblog5k.CSV with delimiter ',' CSV header


2)  Use Spark to extract existing relational data, transform data to Parquet/JSON/CSV data, then write transformed data to the mounted S3 bucket

Scala Notebook (SSL disabled):
val jdbcDF = spark.read
  .format("jdbc")
  .option("url", "jdbc:postgresql://aa.bbb.cc.ddd:5433/logs")
  .option("dbtable", "server1.weblog")
  .option("user", "yugabyte")
  .load()

jdbcDF.printSchema
jdbcDF.show(10)

jdbcDF.coalesce(1).write.format("parquet").mode("overwrite").save("dbfs:/mnt/s3-jj-chaos/weblogs/parquet")

jdbcDF.coalesce(1).write.format("json").mode("overwrite").save("dbfs:/mnt/s3-jj-chaos/weblogs/json")

jdbcDF.coalesce(1).write.option("header", "true").format("csv").mode("overwrite").save("dbfs:/mnt/s3-jj-chaos/weblogs/csv")


(4) Spark Jobs
jdbcDF:org.apache.spark.sql.DataFrame
ip:string
datetime:timestamp
url:string
status:integer
root
 |-- ip: string (nullable = true)
 |-- datetime: timestamp (nullable = true)
 |-- url: string (nullable = true)
 |-- status: integer (nullable = true)

+----------+-------------------+--------------------+------+
|        ip|           datetime|                 url|status|
+----------+-------------------+--------------------+------+
|10.128.2.1|2017-11-30 16:00:48|GET /js/vendor/jq...|   200|
|10.128.2.1|2017-11-30 12:10:01|POST /action.php ...|   302|
|10.131.2.1|2017-12-02 16:44:40|GET /bootstrap-3....|   200|
|10.129.2.1|2017-11-30 13:38:43|GET /login.php HT...|   200|
|10.131.2.1|2017-11-30 17:48:27|GET /contestsubmi...|   200|
|10.130.2.1|2017-12-02 04:59:19|      GET / HTTP/1.1|   302|
|10.129.2.1|2017-11-30 17:13:08|POST /pcompile.ph...|   200|
|10.131.0.1|2017-11-30 12:16:49|GET /login.php HT...|   200|
|10.129.2.1|2017-11-30 12:14:07|      GET / HTTP/1.1|   302|
|10.130.2.1|2017-11-30 15:27:17|GET /contest.php ...|   200|
+----------+-------------------+--------------------+------+
only showing top 10 rows

jdbcDF: org.apache.spark.sql.DataFrame = [ip: string, datetime: timestamp ... 2 more fields]


Files can also be compressed with:
.option("compression","gzip")


For example:
jdbcDF.coalesce(1).write.option("compression","gzip")
.format("json").mode("overwrite").save("dbfs:/mnt/s3-jj-chaos/weblogs/json")







Inspect the target files in S3:
 



Transformed results are written to the specified folder which will contain multiple files including: multiple CRC files, a _SUCCESS file, and the desired target file(s). As a multi-partition system, Spark will write the desired target files as a file per partition resulting in multiple parquet, JSON, or CSV files (along with the multiple CRC files and _SUCCESS files). The coalesce(1) function shown above will merge all partitions into a single partition and write a single file (whereas coalesce(4) would denote four partitions/files, for example). Use caution when using coalesce() with larger datasets -- all work is transferred to a single worker which may cause OutOfMemory exceptions. 

repartition() is also an option that provides similar results. repartition() is used to increase or decrease the RDD, DataFrame, or Dataset partitions, whereas coalesce() is used to only decrease the number of partitions in an efficient way -- coalesce() performs better and requires fewer resources. 

In general, one can determine the number of partitions by multiplying the number of CPUs in the cluster by 2, 3, or 4. When writing the data out to a file system, one can also choose a partition size that will create reasonable sized files (e.g. ~100MB). 


6)  Read Parquet Formatted data files from the mounted S3 bucket, transform data to JSON formatted file, then write JSON formatted file back to the mounted S3 bucket

1)  Use Spark to read the Parquet source source file from the mounted S3 bucket, then write the JSON formatted transform back to the mounted S3 bucket

Scala Notebook:
val df = spark.read.parquet("dbfs:/mnt/s3-jj-chaos/weblogs.parquet")
df.printSchema
df.show(10)
df.coalesce(1).write.format("json").mode("overwrite").save("dbfs:/mnt/s3-jj-chaos/weblogs/parquet2json")


Results:
(3) Spark Jobs
df:org.apache.spark.sql.DataFrame = [ip: string, datetime: timestamp ... 2 more fields]

root
 |-- ip: string (nullable = true)
 |-- datetime: timestamp (nullable = true)
 |-- url: string (nullable = true)
 |-- status: integer (nullable = true)

+----------+-------------------+--------------------+------+
|        ip|           datetime|                 url|status|
+----------+-------------------+--------------------+------+
|10.128.2.1|2017-11-30 16:00:48|GET /js/vendor/jq...|   200|
|10.128.2.1|2017-11-30 12:10:01|POST /action.php ...|   302|
|10.131.2.1|2017-12-02 16:44:40|GET /bootstrap-3....|   200|
|10.129.2.1|2017-11-30 13:38:43|GET /login.php HT...|   200|
|10.131.2.1|2017-11-30 17:48:27|GET /contestsubmi...|   200|
|10.130.2.1|2017-12-02 04:59:19|      GET / HTTP/1.1|   302|
|10.129.2.1|2017-11-30 17:13:08|POST /pcompile.ph...|   200|
|10.131.0.1|2017-11-30 12:16:49|GET /login.php HT...|   200|
|10.129.2.1|2017-11-30 12:14:07|      GET / HTTP/1.1|   302|
|10.130.2.1|2017-11-30 15:27:17|GET /contest.php ...|   200|
+----------+-------------------+--------------------+------+
only showing top 10 rows

df: org.apache.spark.sql.DataFrame = [ip: string, datetime: timestamp ... 2 more fields]



2)  Review JSON formatted transform



From file:

{"ip":"10.128.2.1","datetime":"2017-11-30T16:00:48.000Z","url":"GET /js/vendor/jquery-1.12.0.min.js HTTP/1.1","status":200}
{"ip":"10.128.2.1","datetime":"2017-11-30T12:10:01.000Z","url":"POST /action.php HTTP/1.1","status":302}
{"ip":"10.131.2.1","datetime":"2017-12-02T16:44:40.000Z","url":"GET /bootstrap-3.3.7/js/bootstrap.min.js HTTP/1.1","status":200}
{"ip":"10.129.2.1","datetime":"2017-11-30T13:38:43.000Z","url":"GET /login.php HTTP/1.1","status":200}
{"ip":"10.131.2.1","datetime":"2017-11-30T17:48:27.000Z","url":"GET /contestsubmit.php?id=41 HTTP/1.1","status":200}
{"ip":"10.130.2.1","datetime":"2017-12-02T04:59:19.000Z","url":"GET / HTTP/1.1","status":302}
{"ip":"10.129.2.1","datetime":"2017-11-30T17:13:08.000Z","url":"POST /pcompile.php HTTP/1.1","status":200}
{"ip":"10.131.0.1","datetime":"2017-11-30T12:16:49.000Z","url":"GET /login.php HTTP/1.1","status":200}
{"ip":"10.129.2.1","datetime":"2017-11-30T12:14:07.000Z","url":"GET / HTTP/1.1","status":302}
{"ip":"10.130.2.1","datetime":"2017-11-30T15:27:17.000Z","url":"GET /contest.php HTTP/1.1","status":200}




