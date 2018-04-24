import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object DedupedClicks {

 def main(args: Array[String]) {
   

   val spark = SparkSession
   .builder
   .master("yarn")
   .appName("correlation")
   .getOrCreate()
   val sc    = spark.sparkContext
   sc.setLogLevel("ERROR")
   Logger.getLogger("org").setLevel(Level.ERROR)

   println("broadcast threshold is "+spark.conf.get("spark.sql.autoBroadcastJoinThreshold").toInt / 1024 / 1024 ) 


   import spark.implicits._
   import spark.sql
   import org.apache.spark.sql.SaveMode


   val start_dt   = args(0)
   val end_dt     = args(1)


   val txn_path    = s"hdfs://apollo-phx-nn-ha/user/hive/warehouse/mktng.db/me_txns_correlation"
   spark.read.load(txn_path).createOrReplaceTempView("full_txn_df")


   val deduped_clicks_sql = s"""
   select
   event_id,
   event_id_type,
   event_type_id,
   channel_id,
   channel_name,
   rotation_id,
   rotation_name,
   campaign_id,
   campaign_name,
   event_dt,
   event_ts,
   device_type_level1,
   device_type_level2,
   experience_level1,
   experience_level2,
   flex_column_1,
   flex_column_2,
   flex_column_3,
   flex_column_4,
   flex_column_5,
   flex_column_6,
   item_id,
   transaction_id,
   buyer_id,
   item_site_id,
   leaf_categ_id,
   seller_id,
   seller_country_id,
   buyer_country_id,
   fm_buyer_type_cd,
   fm_buyer_type_desc,
   is_first_purchase,
   auct_type_code,
   auct_end_dt,
   bbowa_device_type,
   created_dt,
   created_time,
   bbowa_sec_diff,
   gmb_usd,
   created_dt as ck_dt

   FROM  full_txn_df
   where ck_dt between  '$start_dt' and '$end_dt'
   and   event_type_id in (1,5,8)
   """


   val rNumb = (row_number()
		.over(Window.partitionBy(col("event_id"), col("item_id"), col("transaction_id"))
		      .orderBy(col("device_type_level1"), col("device_type_level2"),col("experience_level1"),col("experience_level2"),col("flex_column_1"), col("flex_column_3"))
		      )
		)
		
		      
  
   val full_join_df = (spark
		       .sql(deduped_clicks_sql)
		       .select($"*", rNumb as "rnum")
		       ).filter($"rnum"===1)
     
  
     println(deduped_clicks_sql)

   //val full_join_df = spark.sql(deduped_clicks_sql)

   (full_join_df
    .write
    .partitionBy("ck_dt")
    .mode(SaveMode.Append)
    .save("hdfs://apollo-phx-nn-ha/user/hive/warehouse/attrib.db/deduped_clicks_parquet")
  )




 }
}
