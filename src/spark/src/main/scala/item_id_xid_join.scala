import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object itemid_xid_join {

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


   val item_start_dt   = args(0)
   val item_end_dt     = args(1)


   //===========================
   // XID 
   //===========================
   val inc_master_df   = spark.read.load("hdfs://apollo-phx-nn-ha/user/hive/warehouse/mktng.db/inc_master_low_skew_parquet")
   val cguid_to_xid_df = spark.read.load("hdfs://apollo-phx-nn-ha/user/hive/warehouse/mktng.db/incdata_cguid_to_xid_low_skew_parquet")
   val did_to_xid_df   = spark.read.load("hdfs://apollo-phx-nn-ha/user/hive/warehouse/mktng.db/incdata_did_to_xid_low_skew_parquet")

   inc_master_df.createOrReplaceTempView("inc_master")
   cguid_to_xid_df.createOrReplaceTempView("cguid")
   did_to_xid_df.createTempView("did")



   //============================
   //ITEM_ID
   //============================
   spark.read.load("hdfs://apollo-phx-nn-ha/user/hive/warehouse/mktng.db/view_items_cats_parquet_external_ana").createOrReplaceTempView("full_item_df")
   //============================


   
   val all_join_sql = """
   select
   coalesce(A.guid,B.guid) as guid,
   coalesce(A.cguid,B.cguid) as cguid,
   coalesce(A.user_id,B.user_id) as user_id,
   coalesce(A.session_skey,B.session_skey) as session_skey,
   coalesce(A.session_start_dt,B.session_start_dt) as session_start_dt,
   coalesce(A.page_id,B.page_id) as page_id,
   coalesce(A.seqnum,B.seqnum) as seqnum,
   coalesce(A.event_timestamp,B.event_timestamp) as event_timestamp,
   coalesce(A.item_id,B.item_id) as item_id,
   coalesce(A.site_id,B.site_id) as site_id,
   coalesce(A.session_traffic_source_id,B.session_traffic_source_id) as session_traffic_source_id,
   coalesce(A.rotation_id,B.rotation_id) as rotation_id,
   coalesce(A.rover_id,B.rover_id) as rover_id,
   coalesce(A.device_type,B.device_type) as device_type,
   coalesce(A.device_type_level1,B.device_type_level1) as device_type_level1,
   coalesce(A.device_type_level2,B.device_type_level2) as device_type_level2,
   coalesce(A.experience_level1,B.experience_level1) as experience_level1,
   coalesce(A.experience_level2,B.experience_level2) as experience_level2,
   coalesce(A.vi_cnt,B.vi_cnt) as vi_cnt,
   coalesce(A.start_timestamp,B.start_timestamp) as start_timestamp,
   coalesce(A.end_timestamp,B.end_timestamp) as end_timestamp,
   coalesce(A.sec_diff,B.sec_diff) as sec_diff,
   coalesce(A.leaf_categ_id,B.leaf_categ_id) as leaf_categ_id,
   coalesce(A.item_site_id,B.item_site_id) as item_site_id,
   coalesce(A.leaf_categ_name,B.leaf_categ_name) as leaf_categ_name,
   coalesce(A.bsns_vrtcl_name,B.bsns_vrtcl_name) as bsns_vrtcl_name,
   coalesce(A.meta_categ_id,B.meta_categ_id) as meta_categ_id,
   coalesce(A.meta_categ_name,B.meta_categ_name) as meta_categ_name,
   coalesce(A.categ_lvl2_id,B.categ_lvl2_id) as categ_lvl2_id,
   coalesce(A.categ_lvl3_id,B.categ_lvl3_id) as categ_lvl3_id,
   coalesce(A.categ_lvl4_id,B.categ_lvl4_id) as categ_lvl4_id,
   coalesce(A.categ_lvl5_id,B.categ_lvl5_id) as categ_lvl5_id,
   coalesce(A.categ_lvl6_id,B.categ_lvl6_id) as categ_lvl6_id,
   coalesce(A.categ_lvl7_id,B.categ_lvl7_id) as categ_lvl7_id,
   coalesce(A.fm_buyer_type_cd,B.fm_buyer_type_cd) as fm_buyer_type_cd,
   coalesce(A.buyer_type_desc,B.buyer_type_desc) as buyer_type_desc,
   coalesce(A.incdata_id,B.incdata_id) as incdata_id,
   coalesce(A.join_strategy, B.join_strategy) as join_strategy,
   coalesce(A.dt, B.dt) as dt
   
   from uid_join A
   full outer join cguid_join B
   on   A.guid         = B.guid
   and  A.session_skey = B.session_skey
   and  A.item_id      = B.item_id
   and  A.seqnum       = B.seqnum
   """





   
   val item_query = s"""select * from full_item_df 
   where dt between '${item_start_dt}' and '${item_end_dt}' 
   """
   
   
   val uid_join_query = s"""
   select 
   A.*,
   incdata_id,
   'uid' as join_strategy
   from item_non_null_uid   A
   left join inc_master B
   on A.user_id = B.incdata_srcid 
   where dt between '${item_start_dt}' and '${item_end_dt}'
   """

   val cguid_join_query = s"""
   select 
   A.*,
   incdata_id,
   'cguid' as join_strategy
     from item_df   A
   left join cguid B
     on A.cguid = B.ident_attr_value_txt 
     where dt between '${item_start_dt}' and '${item_end_dt}'
   """  

   spark.sql(item_query).createOrReplaceTempView("item_df")
   spark.sql("select * from item_df where user_id > 0").createOrReplaceTempView("item_non_null_uid")
   //NON-NULL UID
   spark.sql(uid_join_query).createOrReplaceTempView("uid_join")
   //NON-NULL CGUID
   spark.sql(cguid_join_query).createOrReplaceTempView("cguid_join")
   //QUALIFIER
   val rNumb = (row_number()
            .over(Window.partitionBy(col("guid"), col("session_skey"), col("item_id"),col("seqnum"))
		    .orderBy(col("device_type_level1"), col("device_type_level2"),col("experience_level1"),col("experience_level2"))
		  )
            )

   val path="hdfs://apollo-phx-nn-ha/user/hive/warehouse/mktng.db/view_items_xid_parquet_v2"     

   val full_join_df = (spark
		       .sql(all_join_sql)
		       .select($"*", rNumb as "rnum")
		       ).filter($"rnum"===1)
     
  
   (full_join_df
    .write
    .partitionBy("dt")
    .mode(SaveMode.Append)
    .save(path)
  )
 }
}
