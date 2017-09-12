
/* SimpleApp.scala */
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession


object CorrelationApp {

 def main(args: Array[String]) {

    val spark = SparkSession.builder.appName("correlation").master("yarn").getOrCreate()

   import spark.implicits._
   import spark.sql

   val txns_path = "hdfs://apollo-phx-nn-ha/user/hive/warehouse/txns.db/txns_with_xid_parquet/dt=2017-07-01"

   val mktng_path = "hdfs://apollo-phx-nn-ha/user/hive/warehouse/mktng.db/marketing_events_non_null_uid_parquet"
      
   val txns_df = spark.read.load(txns_path)
   //#val mktng_df = spark.read.load(mktng_path).filter("user_id is not null")
   //val mktng_df = spark.read.format("orc").load(mktng_path).filter("user_id is not null")
   
   val mktng_df  = spark.read.load(mktng_path)

   txns_df.createOrReplaceTempView("txns")
   mktng_df.createOrReplaceTempView("mktng")

   val query = """
   select
   A.guid,
   A.cguid,
   A.user_id,
   A.event_id,
   A.event_id_type,
   A.event_type_id,
   A.channel_id,
   A.channel_name,
   A.rotation_id,
   A.rotation_name,
   A.campaign_id,
   A.campaign_name,
   A.event_dt,
   A.event_ts,
   A.incdata_id as mktng_incdata_id,
   A.cluster_id as mktng_clustr_id,
   A.device_id,
   A.experience_level1,
   A.experience_level2,
   A.device_level1,
   A.device_level2,
   A.flex_column_1,
   A.flex_column_2,
   A.flex_column_3,
   A.flex_column_4,
   A.flex_column_5,
   A.flex_column_6,
   B.item_id,
   B.transaction_id,
   B.auct_end_dt,
   B.item_site_id,
   B.auct_type_code,
   B.leaf_categ_id,
   B.seller_id,
   B.buyer_id,
   B.created_dt,
   B.created_time,
   B.gmb_usd,
   B.sess_guid,
   B.sess_cguid,
   B.sess_session_skey,
   B.sess_session_start_dt,
   B.sess_site_id,
   B.sess_cobrand,
   B.bbowa_guid,
   B.bbowa_cguid,
   B.bbowa_session_skey,
   B.bbowa_session_start_dt,
   B.bbowa_event_timestamp,
   B.bbowa_site_id,
   B.bbowa_cobrand,
   B.experience_level1 as bbowa_experience_level1,
   B.experience_level2 as bbowa_experience_level2,
   B.bbowa_device_type_level1,
   B.bbowa_device_type_level2,
   B.bbowa_traffic_source_id,
   B.sess_exp_level1,
   B.sess_exp_level2,
   B.sess_device_type_level1,
   B.sess_device_type_level2,
   B.gaid,
   B.idfa,
   B.incdata_id,
   B.incdata_dqscore,
   "uid" as join_strategy

   FROM  txns B
   inner join  mktng A
   on    A.user_id = B.buyer_id

   where   A.dt <= B.dt 
   and   A.event_dt <= B.bbowa_session_start_dt
   and   A.event_ts <= B.bbowa_event_timestamp 
   """
   
   val join_df = sql(query)

   join_df.write.save("hdfs://apollo-phx-nn-ha/user/hive/warehouse/mktng.db/correlation_rover_non_null_uid.parquet")

   spark.stop()
  }
}
