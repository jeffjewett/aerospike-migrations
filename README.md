# Databricks and Apache Spark as an Independent and Uniform Data Migration Platform

The following examples illustrate how Databricks and Spark can be deployed to migrate and/or transform data between formats, independent sources, and/or independent targets. 

Specifically in these examples: 

- Read existing relational data directly from a PostgreSQL-compatible database into a Spark DataFrame, then write the DataFrame to an S3 bucket as Parquet, CSV or JSON formatted data.
- Read Parquet formatted data files from an S3 bucket into a Spark DataFrame, then write the DataFrame to an S3 bucket as CSV or JSON formatted data.
- Read existing relational data directly from a Snowflake database into a Spark DataFrame, perform column transformations, then write the DataFrame directly to an Aerospike database.

Other workflows can then be derived from the above examples, such as:
- Read existing relational data directly from a PostgreSQL-compatible database into a Spark DataFrame, perform column transformations on the implied schema, then write the DataFrame directly to an Aerospike database.
- Read existing relational data directly from a Snowflake database into a Spark DataFrame, write the DataFrame to an S3 bucket as Parquet, CSV or JSON formatted data, read the Parquet formatted data files from an S3 bucket into a Spark DataFrame, then write the DataFrame directly to an Aerospike database.
- Derive Spark connectivity to other database platforms.

Both Databricks and Apache Spark clusters are used in the examples. Scala notebooks/scripts only are presented.

## System Configuration

#### Databricks:
- Databricks Enterprise Edition:
  - 7.6 (includes Apache Spark 3.0.1, Scala 2.12)
- AWS, us-west-2a
- 1 node: m5d.large

#### Apache Spark:
- 3 nodes: c5d.large

#### PostgreSQL:
- Yugabyte Enterprise 2.7.0:
  - GCP
  - Three n1-standard-8 nodes
  - Replication Factor: 3 (RF=3)
  - 1 zone (us-central1), and 3 availability zones (a, b, c)
  - Encryption in-transit enabled
  - OpenSSL version 2.8.3
Note: Yugabyte access port is 5433 (vs well-known 5432)

#### Aerospike:
- AWS, us-west-2a us-west-2b, us-west-2d
- 3 nodes: c5d.large

## SSL
Encryption-in-transit (EIT)  is often enabled to secure the source database. This example also illustrates how to configure the client java JDBC PostgreSQL driver to access EIT-enabled sources. 

To make the certificates compatible with Java in Databricks, the client certificate and key file must be converted to DER format:

#### (1) Client certificate (client_cert.crt):

bash:
```bash
openssl x509 -in client_cert.crt -out client_cert.crt.der -outform der
```

#### (2) Client key files (key_file.key):

bash:
```bash
openssl pkcs8 -topk8 -outform der -in key_file.key -out key_file.key.pk8 -nocrypt
```

#### (3) Note:  The root certificate does not need conversion (root.crt).

#### (4) Install databricks-cli (if not installed):

bash:
```bash
pip install databricks-cli
```

#### (5) Create a certificate folder in Databricks DBFS and copy the following files to that folder (/dbfs/path-to-certs/) (refer to the [Databricks documentation](https://docs.databricks.com/data/databricks-file-system.html) for details):
  - client_cert.crt.der
  - key_file.key.pk8
  - root.crt

#### (6) In the Databricks Scala notebook, set the following JDBC Driver options in either of the following ways:

Scala Notebook:
```scala
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
```scala
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

If SSL is enabled on the source database, substitute either of the above read statements for those shown in the Scala Notebooks, below.

## Mount an S3 bucket in Databricks

#### (1) Create an AWS S3 API User

From AWS IAM console => Users
![aws-iam-i1](https://user-images.githubusercontent.com/12547186/156036668-b57bfde2-298f-44de-ae27-b142b6210f57.png)
=> `Add users`

Create username, Select AWS access type: check Access Key - Programmatic access
![aws-iam-i2](https://user-images.githubusercontent.com/12547186/156038965-7b307d36-be28-43dc-ab3e-2c5b3d98eaa9.png)
=> `Next: Permissions`

Select Attach existing policies directly => AmazonS3FullAccess
![aws-iam-i3](https://user-images.githubusercontent.com/12547186/156039008-2144afb1-0846-46fa-8f64-d59f37ef3fe8.png)
=> `Next: Tags`

Assign tags (for reference)
![aws-iam-i4](https://user-images.githubusercontent.com/12547186/156039272-2b9b76ef-33b1-49ed-976b-c68bf111e9ea.png)
=> `Next: Review`
  
Finally...
![aws-iam-i5](https://user-images.githubusercontent.com/12547186/156039416-266575b3-d8eb-4fd1-8d60-b07e499cbee8.png)
=> `Create user`

Access key ID and Secret access key must be added to a Databricks Secret Scope detailed in step 3), below. Copy and save...
![aws-iam-i6](https://user-images.githubusercontent.com/12547186/156039446-fe7adfbb-fa5c-434d-8a12-8718ef062ef3.png)
=> `Close`

#### (2) Create a Databricks Access Token

From the workspace, select Settings => User Settings
![databricks-token-i1](https://user-images.githubusercontent.com/12547186/156044445-cffab914-7e6c-4912-a3dd-c8816bc5f908.png)

 From the User Setting Page, select the Access Tokens tab -> Generate New Token.  Enter a comment/name.
![databricks-token-i2](https://user-images.githubusercontent.com/12547186/156044541-be34cc12-258f-49a0-882d-d414ffd96fe4.png)
=> `Generate`

Copy the token presented as it will no longer be visible/retrievable once Done is selected.  This token will be required for CLI access, described below. Copy and save for later use.
![databricks-token-i3](https://user-images.githubusercontent.com/12547186/156044666-702984a2-d0f7-41a5-aadd-f4b5169ccb40.png)
=> `Done`

#### (3) Create a Databricks Secrets Scope for AWS credentials

bash:
```bash
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

#### (4) Mount the S3 bucket in Databricks

Scala Notebook:
```scala  
val AccessKey = dbutils.secrets.get(scope = "aws", key = "aws-access-key")
// Encode the Secret Key as that can contain "/"
val SecretKey = dbutils.secrets.get(scope = "aws", key = "aws-secret-key")
val EncodedSecretKey = SecretKey.replace("/", "%2F")
val AwsBucketName = "jj-bucket"
val MountName = "s3-jj-bucket"

dbutils.fs.mount(s"s3a://$AccessKey:$EncodedSecretKey@$AwsBucketName", s"/mnt/$MountName")
display(dbutils.fs.ls(s"/mnt/$MountName"))
```
Results:
| | Path | Name | Size |
| ---- | ---- | ---- | ---- |
| 1 | dbfs:/mnt/s3-jj-bucket/coalesce-csv | coalesce-csv | 0 |
| 2 | dbfs:/mnt/s3-jj-bucket/weblogs | weblogs | 0 |
  
(note: subfolder 'coalesce-csv' and 'weblogs' created in AWS jj-bucket)
  
_The mount will remain in-scope for the lifetime of the workspace (until deleted)._

## Extract Existing PostgreSQL Data and Transform to Parquet/JSON/CSV Formatted Data

#### (1) Load Data

Sample data (CSV):
```csv  
IP,Time,URL,Status
10.128.2.1,29-Nov-2017 06:58:55,GET /login.php HTTP/1.1,200 
10.128.2.1,29-Nov-2017 06:59:02,POST /process.php HTTP/1.1,302 
10.128.2.1,29-Nov-2017 06:59:03,GET /home.php HTTP/1.1,200
10.131.2.1,29-Nov-2017 06:59:04,GET /js/vendor/moment.min.js HTTP/1.1,200 
10.130.2.1,29-Nov-2017 06:59:06,GET /bootstrap-3.3.7/js/bootstrap.js HTTP/1.1,200 
```
  
[Download 'weblog.csv' sample data for this example here](https://www.kaggle.com/shawon10/web-log-dataset)

SQL shell:
```sql
create database logs;
\c logs;
create schema server1;
set schema 'server1';
create table weblog(ip text, datetime timestamp, url text, status int);
\copy weblog from weblog5k.CSV with delimiter ',' CSV header
```

#### (2) Use Spark to extract existing relational data, transform data to Parquet/JSON/CSV data, then write transformed data to the mounted S3 bucket

Scala Notebook (SSL disabled):
```scala
val jdbcDF = spark.read
  .format("jdbc")
  .option("url", "jdbc:postgresql://aa.bbb.cc.ddd:5433/logs")
  .option("dbtable", "server1.weblog")
  .option("user", "yugabyte")
  .load()

jdbcDF.printSchema
jdbcDF.show(10)

jdbcDF.coalesce(1).write.format("parquet").mode("overwrite").save("dbfs:/mnt/s3-jj-bucket/weblogs/parquet")

jdbcDF.coalesce(1).write.format("json").mode("overwrite").save("dbfs:/mnt/s3-jj-bucket/weblogs/json")

jdbcDF.coalesce(1).write.option("header", "true").format("csv").mode("overwrite").save("dbfs:/mnt/s3-jj-bucket/weblogs/csv")
```
Output:
```
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
```
Files can also be compressed with:
  ```scala
.option("compression","gzip")
  ```
For example:
  ```scala
jdbcDF.coalesce(1).write.option("compression","gzip")
.format("json").mode("overwrite").save("dbfs:/mnt/s3-jj-bucket/weblogs/json")
```

#### (3) Inspect the target files in S3

![s3-i1](https://user-images.githubusercontent.com/12547186/156080867-9441a1c8-a182-4046-bbf8-f74629b309de.png)
![s3-i2](https://user-images.githubusercontent.com/12547186/156081340-106a85a3-ae61-45bb-b5ea-b9ef06d30c81.png)
![s3-i3](https://user-images.githubusercontent.com/12547186/156081483-d6fe84d4-4a0d-446b-bffa-e40658b1efac.png)

Transformed results are written to the specified folder which will contain multiple files including: multiple CRC files, a _SUCCESS file, and the desired target file(s). As a multi-partition system, Spark will write the desired target files as a file per partition resulting in multiple parquet, JSON, or CSV files (along with the multiple CRC files and _SUCCESS files). The coalesce(1) function shown above will merge all partitions into a single partition and write a single file (whereas coalesce(4) would denote four partitions/files, for example). Use caution when using coalesce() with larger datasets -- all work is transferred to a single worker which may cause OutOfMemory exceptions. 

repartition() is also an option that provides similar results. repartition() is used to increase or decrease the RDD, DataFrame, or Dataset partitions, whereas coalesce() is used to only decrease the number of partitions in an efficient way -- coalesce() performs better and requires fewer resources. 

In general, one can determine the number of partitions by multiplying the number of CPUs in the cluster by 2, 3, or 4. When writing the data out to a file system, one can also choose a partition size that will create reasonable sized files (e.g. ~100MB). 

## Read Parquet Formatted data files from the mounted S3 bucket, transform data to JSON formatted file, then write JSON formatted file back to the mounted S3 bucket

#### (1) Use Spark to read the Parquet source source file from the mounted S3 bucket, then write the JSON formatted transform back to the mounted S3 bucket

Scala Notebook:
  ```scala
val df = spark.read.parquet("dbfs:/mnt/s3-jj-bucket/weblogs.parquet")
df.printSchema
df.show(10)
df.coalesce(1).write.format("json").mode("overwrite").save("dbfs:/mnt/s3-jj-bucket/weblogs/parquet2json")
```
Output:
```
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
```

#### (2) Review JSON formatted transform

From file:
```
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
```

## Extract Existing Snowflake Data and Migrate Directly to Aerospike

The example that follows will migrate existing Snowflake data (Snowflake TPC-H sample data, 'customer' table) directly to an Areospike database using Apache Spark. The Snowflake data will be read into a Spark DataFrame, the key column of the _implied_ schema of the key column will be transformed, then the entire DataFrame will be written directly to Aerospike.  Both the Apache Spark and Aerospike clusters will be provisiond with Ansible scripts found found [here](https://github.com/aerospike-examples/aerospike-ansible). A provisioning tutorial using these scripts can be found [here](https://dev.to/aerospike/using-aerospike-connect-for-spark-3poi).

#### (1) Enable Aerospike Enterprise

Modify ~/aerospike-ansible/vars/cluster-config.yml:
```yaml
# Whether to use enterprise
# Set to false by default so will run without features.conf
enterprise: true
feature_key: ~/aerospike-ansible/assets/features.conf
```

#### To ssh/scp from a machine that is not the ansible project root:

- copy `aerospike.aws.pem` from `~/aerospike-ansible` to machine
- on machine terminal: `chmod 400 aerospike.aws.pem`
- get public ip from `~/aerospike-ansible/scripts/cluster-ip-address-list.sh`

#### (2) ssh into one of the Spark nodes
```bash
~/aerospike-ansible/scripts/spark-quick-ssh.sh
```

#### (3) Start the Spark Shell with Snowflake drivers (Using Spark master internal IP noted when cluster was provisioned)
[ec2-user@ip-10-0-0-123 ~]$ /spark/bin/spark-shell --master spark://10.0.0.123:7077 --packages net.snowflake:snowflake-jdbc:3.12.17,net.snowflake:spark-snowflake_2.12:2.8.4-spark_3.0

#### (4) Whitelist public ip’s of Spark Cluster in Snowflake security
https://app.snowflake.com/us-region-x/abc12345/account/security

IP's of the Spark nodes can be found with the following:
```bash
~/aerospike-ansible/scripts/spark-ip-address-list.sh 
```
Three entries will be shown:
```
spark IP addresses : Public : 111.222.333.444, Private : 10.0.0.123
spark IP addresses : Public : 222.333.444.555, Private : 10.0.1.123
spark IP addresses : Public : 333.444.555.666, Private : 10.0.2.123
```

#### (5) AN Aerospike seed node IP will need to be specified (in write script)

IP's of the Aerospike nodes can be found with the following:
```bash
~/aerospike-ansible/scripts/cluster-ip-address-list.sh 
```
Three entries will be shown:
```
cluster IP addresses : Public : 111.222.333.444, Private : 10.0.0.123
cluster IP addresses : Public : 222.333.444.555, Private : 10.0.1.123
cluster IP addresses : Public : 333.444.555.666, Private : 10.0.2.123
```

#### (6) Read Snowflake data into Spark DataFrame

Scala script:
```scala
import org.apache.spark.sql.{ SQLContext, SparkSession, SaveMode}
import com.aerospike.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

val sfoptions = Map( 
  "sfUrl" -> "abc12345.snowflakecomputing.com",
  "sfUser" -> "username",
  "sfPassword" -> "password",
  "sfDatabase" -> "snowflake_sample_data",
  "sfSchema" -> "tpch_sf1",
  "sfWarehouse" -> "COMPUTE_WH")

val df = spark.read
  .format("snowflake")
  .options(sfoptions)
  .option("dbtable", "customer")
  .load()

df.printSchema
df.show(10)
```
Output:
```scala
(1) Spark Jobs
Job 1 View(Stages: 1/1)
Stage 1: 1/1   
df:org.apache.spark.sql.DataFrame
C_CUSTKEY:decimal(38,0)
C_NAME:string
C_ADDRESS:string
C_NATIONKEY:decimal(38,0)
C_PHONE:string
C_ACCTBAL:decimal(12,2)
C_MKTSEGMENT:string
C_COMMENT:string
root
 |-- C_CUSTKEY: decimal(38,0) (nullable = false)
 |-- C_NAME: string (nullable = false)
 |-- C_ADDRESS: string (nullable = false)
 |-- C_NATIONKEY: decimal(38,0) (nullable = false)
 |-- C_PHONE: string (nullable = false)
 |-- C_ACCTBAL: decimal(12,2) (nullable = false)
 |-- C_MKTSEGMENT: string (nullable = true)
 |-- C_COMMENT: string (nullable = true)

+---------+------------------+--------------------+-----------+---------------+---------+------------+--------------------+
|C_CUSTKEY|            C_NAME|           C_ADDRESS|C_NATIONKEY|        C_PHONE|C_ACCTBAL|C_MKTSEGMENT|           C_COMMENT|
+---------+------------------+--------------------+-----------+---------------+---------+------------+--------------------+
|    60001|Customer#000060001|          9Ii4zQn9cX|         14|24-678-784-9652|  9957.56|   HOUSEHOLD|l theodolites boo...|
|    60002|Customer#000060002|    ThGBMjDwKzkoOxhz|         15|25-782-500-8435|   742.46|    BUILDING| beans. fluffily ...|
|    60003|Customer#000060003|Ed hbPtTXMTAsgGhC...|         16|26-859-847-7640|  2526.92|    BUILDING|fully pending dep...|
|    60004|Customer#000060004|NivCT2RVaavl,yUnK...|         10|20-573-674-7999|  7975.22|  AUTOMOBILE| furiously above ...|
|    60005|Customer#000060005|1F3KM3ccEXEtI, B2...|         12|22-741-208-1316|  2504.74|   MACHINERY|express instructi...|
|    60006|Customer#000060006|      3isiXW651fa8p |         22|32-618-195-8029|  9051.40|   MACHINERY| carefully quickl...|
|    60007|Customer#000060007|sp6KJmx,TiSWbMPvh...|         12|22-491-919-9470|  6017.17|   FURNITURE|bold packages. re...|
|    60008|Customer#000060008|3VteHZYOfbgQioA96...|          2|12-693-562-7122|  5621.44|  AUTOMOBILE|nal courts. caref...|
|    60009|Customer#000060009|S60sNpR6wnacPBLeO...|          9|19-578-776-2699|  9548.01|   FURNITURE|efully even depen...|
|    60010|Customer#000060010|c4vEEaV1tdqLdw2oV...|         21|31-677-809-6961|  3497.91|   HOUSEHOLD|fter the quickly ...|
+---------+------------------+--------------------+-----------+---------------+---------+------------+--------------------+
only showing top 10 rows
```

#### (7) Write Spark DataFrame directly to Aerospike database

Aerospike primary keys must be of types integer, string, or blob.  As implied schema C_CUSTKEY is of type __decimal__, C_CUSTKEY will need to be transformed.

```scala
val df2 = df.withColumn("C_CUSTKEY",col("C_CUSTKEY").cast(StringType))
df2.printSchema()
```
Output:
```scala
 |-- C_CUSTKEY: string (nullable = false)
 |-- C_NAME: string (nullable = false)
 |-- C_ADDRESS: string (nullable = false)
 |-- C_NATIONKEY: decimal(38,0) (nullable = false)
 |-- C_PHONE: string (nullable = false)
 |-- C_ACCTBAL: decimal(12,2) (nullable = false)
 |-- C_MKTSEGMENT: string (nullable = true)
 |-- C_COMMENT: string (nullable = true)
 ```

```scala
// write to Aerospike
df2.write.mode(SaveMode.Overwrite).format("aerospike").option("aerospike.seedhost", "10.0.0.123:3000").option("aerospike.namespace", "test").option("aerospike.set", "sf-tpch").option("aerospike.updateByKey", "C_CUSTKEY").save()

// IP '10.0.0.123' from ~/aerospike-ansible/scripts/cluster-ip-address-list.sh
```

Complete Sacla script:
```scala
import org.apache.spark.sql.{ SQLContext, SparkSession, SaveMode}
import com.aerospike.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

// Read from Snowflake
val sfoptions = Map( 
  "sfUrl" -> "abc12345.snowflakecomputing.com",
  "sfUser" -> "username",
  "sfPassword" -> "password",
  "sfDatabase" -> "snowflake_sample_data",
  "sfSchema" -> "tpch_sf1",
  "sfWarehouse" -> "COMPUTE_WH")

val df = spark.read
  .format("snowflake")
  .options(sfoptions)
  .option("dbtable", "customer")
  .load()
  
// Write to Aerospike
val df2 = df.withColumn("C_CUSTKEY",col("C_CUSTKEY").cast(StringType))

df2.write.mode(SaveMode.Overwrite).format("aerospike").option("aerospike.seedhost", "10.0.0.123:3000").option("aerospike.namespace", "test").option("aerospike.set", "sf-tpch").option("aerospike.updateByKey", "C_CUSTKEY").save()
```

Confirm data in Aerospike...
