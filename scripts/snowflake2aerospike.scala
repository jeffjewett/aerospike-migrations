mport org.apache.spark.sql.{ SQLContext, SparkSession, SaveMode}
import com.aerospike.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.IntegerType

//
// specify user, password, account-id
//
val sfoptions = Map(
  "sfUrl"->"sf_acct_id.snowflakecomputing.com",
  "sfUser"->"user",
  "sfPassword"->"password",
  "sfDatabase"->"WORKSPACE_DB",
  "sfSchema"->"WORKSPACE",
  "sfWarehouse"->"COMPUTE_WH")

val dfall = spark.read
  .format("snowflake")
  .options(sfoptions)
  .option("dbtable", "PEOPLE_PROFILES")
  .load()
    .select(col("PERSON_ID"), split(regexp_replace(col("AUDIENCE_ARRAY"),"[\\[\\]]",""),",")
    .cast("array<long>").as("AUDIENCE_ARR_T"))
    .drop("AUDIENCE_ARRAY")

dfall.write
  .mode(SaveMode.Overwrite)
  .format("aerospike")
  .option("aerospike.seedhost", "10.0.1.7:3000")
  .option("aerospike.namespace", "test")
  .option("aerospike.set", "choozle-demo")
  .option("aerospike.updateByKey", "PERSON_ID")
  .option("aerospike.sendKey", "true")
  .save()
