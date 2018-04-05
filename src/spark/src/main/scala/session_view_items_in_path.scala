import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.Column


object Sess_VI_in_PATH {

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



   // ========================
   val sess_start_dt   = args(0)
   val sess_end_dt     = args(1)
   val items_start_dt = args(2)
   val items_end_dt   = sess_end_dt
   // ========================


   // ========================
   // SESSIONS
   // ========================
   val sess_path = "hdfs://apollo-phx-nn-ha/user/hive/warehouse/mktng.db/sess_cko_bbowa_vi_with_xid"
   // ========================

   // ========================   
   //SESSIONS
   // ========================
   val sessions_df = spark.read.load("hdfs://apollo-phx-nn-ha/user/hive/warehouse/mktng.db/clav_sess_parquet")
   sessions_df.createOrReplaceTempView("sessions_all")
   spark.sql(s"select * from sessions_all where dt between '${sess_start_dt}' and '${sess_end_dt}'").createOrReplaceTempView("sessions")



   //=========================
   //VIEW ITEMS 
   //=========================
   val items_path = s"hdfs://apollo-phx-nn-ha/user/hive/warehouse/mktng.db/view_items_cats_parquet_external_ana"
   spark.read.load(items_path).createOrReplaceTempView("items_all")
   spark.sql(s"select * from items_all where dt between '$items_start_dt' and '$items_end_dt'").createOrReplaceTempView("items")   
   //
   //
   // ========================
   //LOAD FRAMES
   // ========================
   //val sess_xid_df   = spark.read.load(sess_path)
   //val sess_df       = sess_xid_df.filter(s"dt between '$sess_start_dt' and '$sess_end_dt'").sample(false, 0.2)
   //sess_df.cache()
   //sess_df.createOrReplaceTempView("sess_xid")
   // ========================   
   
   

   // ========================
   val common_join_sql = """
   select
   A.guid as sess_guid,
   A.cguid as sess_cguid,
   A.parent_uid as sess_parent_uid,
   A.session_skey as sess_session_skey,
   A.site_id as sess_site_id,
   A.cobrand as sess_cobrand,
   A.session_start_dt as sess_session_start_dt,
   A.start_timestamp as sess_start_timestamp,
   A.end_timestamp as sess_end_timestamp,
   A.primary_app_id as sess_primary_app_id,
   A.session_traffic_source_id as sess_session_traffic_source_id,
   A.cust_traffic_source_level1 as sess_cust_traffic_source_level1,
   A.cust_traffic_source_level2 as sess_cust_traffic_source_level2,
   A.traffic_source_level3 as sess_traffic_source_level3,
   A.rotation_id as sess_rotation_id,
   A.rvr_id as sess_rvr_id,
   A.idfa as sess_idfa,
   A.gadid as sess_gadid,
   A.gdid as sess_gdid,
   A.device_id as sess_device_id,
   A.device_type as sess_device_type,
   A.device_type_level1 as sess_device_type_level1,
   A.device_type_level2 as sess_device_type_level2,
   A.experience_level1 as sess_experience_level1,
   A.experience_level2 as sess_experience_level2,
   A.vi_cnt as sess_vi_cnt,   
   B.item_id as path_item_id,
   B.event_timestamp as path_event_ts,
   B.bsns_vrtcl_name as path_bsns_vrtcl_name,
   B.meta_categ_id as path_meta_categ_id,
   B.meta_categ_name as path_meta_categ_name,
   B.categ_lvl2_id as path_categ_lvl2_id,
   B.categ_lvl3_id as path_categ_lvl3_id,
   B.categ_lvl4_id as path_categ_lvl4_id,
   """

   //UID
   sql("select * from sessions where parent_uid >0").createOrReplaceTempView("sess_xid_non_null_uid")
   sql("select * from items where user_id >0").createOrReplaceTempView("items_non_null_uid")
   val uid_query_tail = """
   "uid"                 as item_join_strategy

   FROM        sess_xid_non_null_uid A
   INNER JOIN  items_non_null_uid    B
   on          A.parent_uid = B.user_id

   where (B.session_start_dt   <= A.session_start_dt)
   and   (datediff(A.session_start_dt, B.session_start_dt) <= 14)
   and   (unix_timestamp(B.event_timestamp) <= unix_timestamp(A.start_timestamp))
   and   (unix_timestamp(A.start_timestamp) -  unix_timestamp(B.event_timestamp) <= 14*86400)   
   """




   //CGUID
   val cguid_query_tail = """
   "cguid"                 as item_join_strategy

   FROM        sessions              A
   INNER JOIN  items                 B
   on          A.cguid = B.cguid

   where (B.session_start_dt   <= A.session_start_dt)
   and   (datediff(A.session_start_dt, B.session_start_dt) <= 14)
   and   (unix_timestamp(B.event_timestamp) <= unix_timestamp(A.start_timestamp))
   and   (unix_timestamp(A.start_timestamp) -  unix_timestamp(B.event_timestamp) <= 14*86400)   
   """



   sql(common_join_sql+  uid_query_tail).createOrReplaceTempView("uid_join")
   sql(common_join_sql+cguid_query_tail).createOrReplaceTempView("cguid_join")   
   


   val full_join_sql = s"""
   sess_guid,
   sess_cguid,
   sess_parent_uid,
   sess_session_skey,
   sess_site_id,
   sess_cobrand,
   sess_session_start_dt,
   sess_start_timestamp,
   sess_end_timestamp,
   sess_primary_app_id,
   sess_session_traffic_source_id,
   sess_cust_traffic_source_level1,
   sess_cust_traffic_source_level2,
   sess_traffic_source_level3,
   sess_rotation_id,
   sess_rvr_id,
   sess_idfa,
   sess_gadid,
   sess_gdid,
   sess_device_id,
   sess_device_type,
   sess_device_type_level1,
   sess_device_type_level2,
   sess_experience_level1,
   sess_experience_level2,
   sess_vi_cnt,
   path_item_id,
   path_event_ts,
   path_bsns_vrtcl_name,
   path_meta_categ_id,
   path_meta_categ_name,
   path_categ_lvl2_id,
   path_categ_lvl3_id,
   path_categ_lvl4_id,
   item_join_strategy
   
   from              uid_join  A
   FULL OUTER JOIN  cguid_join B
   ON    A.sess_guid         = B.sess_guid
   AND   A.sess_session_skey = B.sess_session_skey
   AND   A.path_item_id      = B.path_item_id
   AND   A.path_event_ts     = B.pah_event_ts

   """
   
   

 }
}


   



