import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel


object SignedInCorrelationApp {

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



   val reg_start_dt   = args(0)
   val reg_end_dt     = args(1)
   val mktng_start_dt = args(2)
   val mktng_end_dt   = reg_end_dt
   val bbowa_start_dt = reg_start_dt
   val bbowa_end_dt   = reg_end_dt
   // ========================


   // ========================
   // M EVENTS
   // ========================
   val mktng_path   = s"hdfs://apollo-phx-nn-ha/user/hive/warehouse/mktng.db/marketing_events_parquet"

   // ========================
   // REGISTRATIONS
   // ========================
   val user_reg_path = "hdfs://apollo-phx-nn-ha/user/hive/warehouse/mktng.db/sessions_signedin_xid_parquet"
   // ========================


   // ========================
   //BBOWA EVENTS
   // ========================
   val bbowa_path  = "hdfs://apollo-phx-nn-ha/user/hive/warehouse/txns.db/bbowa_parquet"
   // ========================

   // ========================
   //LOAD FRAMES
   // ========================
   val mevents_df    = spark.read.load(mktng_path)
   val reg_xid_df    = spark.read.load(user_reg_path)
   val bbowa_full_df = spark.read.load(bbowa_path)

   // ========================
   //FILTER FOR DATES
   // ========================
   val reg_df   = reg_xid_df.filter(s"dt between '$reg_start_dt' and '$reg_end_dt'").sample(false, 0.005)
   val mktng_df = mevents_df.filter(s"dt between '$mktng_start_dt' and '$mktng_end_dt'")
   reg_df.cache()
   //println(reg_df.count())
   reg_df.createOrReplaceTempView("reg_xid")
   mktng_df.createOrReplaceTempView("mktng")

   //===========================
   //BBOWA GROUPBY USER
   //===========================
   spark.read.load(bbowa_path).createOrReplaceTempView("bbowa_all")
   val bbowa_sql  = s"""
   select
   user_id,
   sum(case when event_type = "Watch" then 1 else 0 end ) as num_watch,
   sum(case when event_type = "Ask"   then 1 else 0 end ) as num_ask,
   sum(case when event_type = "AC"    then 1 else 0 end ) as num_ac,
   sum(case when event_type = "BO"    then 1 else 0 end ) as num_bo,
   sum(case when event_type = "BB"    then 1 else 0 end ) as num_bb,
   count(*) as num_bbowa
   from bbowa_all
   where dt between '$bbowa_start_dt' and DATE_ADD('$bbowa_end_dt', 1)
   and user_id > 0
   group by user_id
   """
   spark.sql(bbowa_sql).createOrReplaceTempView("bbowa_grouped")


   val common_join_sql = """
   select
   A.guid,
   A.cguid,
   A.user_id,
   cast(A.event_id as string) as event_id,
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
   A.incdata_id,
   A.cluster_id,
   A.device_id,
   A.experience_level1,
   A.experience_level2,
   A.device_type_level1,
   A.device_type_level2,
   A.flex_column_1,
   A.flex_column_2,
   A.flex_column_3,
   A.flex_column_4,
   A.flex_column_5,
   A.flex_column_6,
   B.guid         as unreg_guid,
   B.cguid        as unreg_cguid,
   B.parent_uid   as parent_uid,
   B.session_skey as unreg_session_skey,
   B.site_id      as unreg_site_id,
   B.cobrand      as unreg_cobrand,
   B.session_start_dt     as unreg_session_start_dt,
   B.start_timestamp     as unreg_start_timestamp,
   B.end_timestamp     as unreg_end_timestamp,
   B.primary_app_id     as unreg_primary_app_id,
   B.session_traffic_source_id     as unreg_session_traffic_source_id,
   B.cust_traffic_source_level1     as unreg_cust_traffic_source_level1,
   B.cust_traffic_source_level2     as unreg_cust_traffic_source_level2,
   B.traffic_source_level3     as unreg_traffic_source_level3,
   B.rotation_id     as unreg_rotation_id,
   B.rvr_id     as unreg_rvr_id,
   B.idfa     as unreg_idfa,
   B.gadid     as unreg_gadid,
   B.gdid         as unreg_gdid,
   B.device_id     as unreg_device_id,
   B.device_type     as unreg_device_type,
   B.device_type_level1     as unreg_device_type_level1,
   B.device_type_level2     as unreg_device_type_level2,
   B.experience_level1     as unreg_experience_level1,
   B.experience_level2     as unreg_experience_level2,
   B.vi_cnt     as unreg_vi_cnt,
   B.incdata_id     as unreg_incdata_id,
   B.join_strategy     as unreg_join_strategy,
   (unix_timestamp(B.start_timestamp) - unix_timestamp(A.event_ts))/86400. as reg_day_diff,
   (unix_timestamp(B.start_timestamp) - unix_timestamp(A.event_ts))        as reg_sec_diff,   
   """

   val cguid_query_tail = """
   "cguid"                 as join_strategy

   FROM        mktng      A
   INNER JOIN  reg_xid    B
   on          A.CGUID = B.CGUID

   where (A.event_dt   <= B.session_start_dt)
   and  (datediff(B.session_start_dt, A.event_dt) <= 14)
   and  (unix_timestamp(A.event_ts) <= unix_timestamp(B.start_timestamp) + 600)
   and  (unix_timestamp(B.start_timestamp) -  unix_timestamp(A.event_ts) <= 14*86400)
   """

   val did_query_tail = """
   "did"                 as join_strategy

   FROM        mktng_non_null_did      A
   INNER JOIN  reg_xid_non_null_did    B
   on          A.device_id = B.device_id
   where (A.event_dt   <= B.session_start_dt)
   and  (datediff(B.session_start_dt, A.event_dt) <= 14)
   and  (unix_timestamp(A.event_ts) <= unix_timestamp(B.start_timestamp) + 600)
   and  (unix_timestamp(B.start_timestamp) -  unix_timestamp(A.event_ts) <= 14*86400)
   
   """

   val xid_query_tail = """
   "xid"                 as join_strategy
   
   FROM        mktng_non_null_xid      A
   INNER JOIN  reg_xid_non_null_xid    B
   on          A.incdata_id = B.incdata_id

   where (A.event_dt   <= B.session_start_dt)
   and  (datediff(B.session_start_dt, A.event_dt) <= 14)
   and  (unix_timestamp(A.event_ts) <= unix_timestamp(B.start_timestamp) + 600)
   and  (unix_timestamp(B.start_timestamp) -  unix_timestamp(A.event_ts) <= 14*86400)   
   """

   sql("""select * from mktng where LENGTH(device_id)>=35 and device_id not like "%00000000-0000-0000-0000-000000000000%" """).createOrReplaceTempView("mktng_non_null_did")

   sql("select * from mktng where incdata_id is not null").createOrReplaceTempView("mktng_non_null_xid")

   sql("""select * from reg_xid where LENGTH(device_id)>=35 and device_id not like "%00000000-0000-0000-0000-000000000000%" """).createOrReplaceTempView("reg_xid_non_null_did")

   sql("select * from reg_xid where incdata_id is not null").createOrReplaceTempView("reg_xid_non_null_xid")

   sql(common_join_sql+cguid_query_tail).createOrReplaceTempView("cguid_join")
   sql(common_join_sql+did_query_tail).createOrReplaceTempView("did_join")
   sql(common_join_sql+xid_query_tail).createOrReplaceTempView("xid_join")   

   //val xid_join_df = sql(common_join_sql+did_query_tail)
   

   //val xid_count = xid_join_df.count()x
   //
   //
   //
   //
   //
   val full_join_sql = """
   select
   coalesce(A.guid,B.guid,C.guid) as guid,
   coalesce(A.cguid,B.cguid,C.cguid) as cguid,
   coalesce(A.user_id,B.user_id,C.user_id) as user_id,
   coalesce(A.event_id,B.event_id,C.event_id) as event_id,
   coalesce(A.event_id_type,B.event_id_type,C.event_id_type) as event_id_type,
   coalesce(A.event_type_id,B.event_type_id,C.event_type_id) as event_type_id,
   coalesce(A.channel_id,B.channel_id,C.channel_id) as channel_id,
   coalesce(A.channel_name,B.channel_name,C.channel_name) as channel_name,
   coalesce(A.rotation_id,B.rotation_id,C.rotation_id) as rotation_id,
   coalesce(A.rotation_name,B.rotation_name,C.rotation_name) as rotation_name,
   coalesce(A.campaign_id,B.campaign_id,C.campaign_id) as campaign_id,
   coalesce(A.campaign_name,B.campaign_name,C.campaign_name) as campaign_name,
   coalesce(A.event_dt,B.event_dt,C.event_dt) as event_dt,
   coalesce(A.event_ts,B.event_ts,C.event_ts) as event_ts,
   coalesce(A.incdata_id,B.incdata_id,C.incdata_id) as incdata_id,
   coalesce(A.cluster_id,B.cluster_id,C.cluster_id) as cluster_id,
   coalesce(A.device_id,B.device_id,C.device_id) as device_id,
   coalesce(A.experience_level1,B.experience_level1,C.experience_level1) as experience_level1,
   coalesce(A.experience_level2,B.experience_level2,C.experience_level2) as experience_level2,
   coalesce(A.device_type_level1,B.device_type_level1,C.device_type_level1) as device_type_level1,
   coalesce(A.device_type_level2,B.device_type_level2,C.device_type_level2) as device_type_level2,
   coalesce(A.flex_column_1,B.flex_column_1,C.flex_column_1) as flex_column_1,
   coalesce(A.flex_column_2,B.flex_column_2,C.flex_column_2) as flex_column_2,
   coalesce(A.flex_column_3,B.flex_column_3,C.flex_column_3) as flex_column_3,
   coalesce(A.flex_column_4,B.flex_column_4,C.flex_column_4) as flex_column_4,
   coalesce(A.flex_column_5,B.flex_column_5,C.flex_column_5) as flex_column_5,
   coalesce(A.flex_column_6,B.flex_column_6,C.flex_column_6) as flex_column_6,

   coalesce(A.unreg_guid,B.unreg_guid,C.unreg_guid) as unreg_guid,
   coalesce(A.unreg_cguid,B.unreg_cguid,C.unreg_cguid) as unreg_cguid,
   coalesce(A.parent_uid,B.parent_uid,C.parent_uid) as parent_uid,
   coalesce(A.unreg_session_skey,B.unreg_session_skey,C.unreg_session_skey) as unreg_session_skey,
   coalesce(A.unreg_site_id,B.unreg_site_id,C.unreg_site_id) as unreg_site_id,
   coalesce(A.unreg_cobrand,B.unreg_cobrand,C.unreg_cobrand) as unreg_cobrand,
   coalesce(A.unreg_session_start_dt,B.unreg_session_start_dt,C.unreg_session_start_dt) as unreg_session_start_dt,
   coalesce(A.unreg_start_timestamp,B.unreg_start_timestamp,C.unreg_start_timestamp) as unreg_start_timestamp,
   coalesce(A.unreg_end_timestamp,B.unreg_end_timestamp,C.unreg_end_timestamp) as unreg_end_timestamp,
   coalesce(A.unreg_primary_app_id,B.unreg_primary_app_id,C.unreg_primary_app_id) as unreg_primary_app_id,
   coalesce(A.unreg_session_traffic_source_id,B.unreg_session_traffic_source_id,C.unreg_session_traffic_source_id) as unreg_session_traffic_source_id,
   coalesce(A.unreg_cust_traffic_source_level1,B.unreg_cust_traffic_source_level1,C.unreg_cust_traffic_source_level1) as unreg_cust_traffic_source_level1,
   coalesce(A.unreg_cust_traffic_source_level2,B.unreg_cust_traffic_source_level2,C.unreg_cust_traffic_source_level2) as unreg_cust_traffic_source_level2,
   coalesce(A.unreg_traffic_source_level3,B.unreg_traffic_source_level3,C.unreg_traffic_source_level3) as unreg_traffic_source_level3,
   coalesce(A.unreg_rotation_id,B.unreg_rotation_id,C.unreg_rotation_id) as unreg_rotation_id,
   coalesce(A.unreg_rvr_id,B.unreg_rvr_id,C.unreg_rvr_id) as unreg_rvr_id,
   coalesce(A.unreg_idfa,B.unreg_idfa,C.unreg_idfa) as unreg_idfa,
   coalesce(A.unreg_gadid,B.unreg_gadid,C.unreg_gadid) as unreg_gadid,
   coalesce(A.unreg_gdid,B.unreg_gdid,C.unreg_gdid) as unreg_gdid,
   coalesce(A.unreg_device_id,B.unreg_device_id,C.unreg_device_id) as unreg_device_id,
   coalesce(A.unreg_device_type,B.unreg_device_type,C.unreg_device_type) as unreg_device_type,
   coalesce(A.unreg_device_type_level1,B.unreg_device_type_level1,C.unreg_device_type_level1) as unreg_device_type_level1,
   coalesce(A.unreg_device_type_level2,B.unreg_device_type_level2,C.unreg_device_type_level2) as unreg_device_type_level2,
   coalesce(A.unreg_experience_level1,B.unreg_experience_level1,C.unreg_experience_level1) as unreg_experience_level1,
   coalesce(A.unreg_experience_level2,B.unreg_experience_level2,C.unreg_experience_level2) as unreg_experience_level2,
   coalesce(A.unreg_vi_cnt,B.unreg_vi_cnt,C.unreg_vi_cnt) as unreg_vi_cnt,
   coalesce(A.unreg_incdata_id,B.unreg_incdata_id,C.unreg_incdata_id) as unreg_incdata_id,
   coalesce(A.unreg_join_strategy, B.unreg_join_strategy, C.unreg_join_strategy) as unreg_join_strategy,
   coalesce(A.reg_day_diff,B.reg_day_diff,C.reg_day_diff) as reg_day_diff,
   coalesce(A.reg_sec_diff,  B.reg_sec_diff,   C.reg_sec_diff ) as reg_sec_diff,   
   coalesce(A.join_strategy, B.join_strategy, C.join_strategy) as join_strategy,
   row_number() over(partition by coalesce(A.unreg_session_skey, B.unreg_session_skey, C.unreg_session_skey),
                                  coalesce(A.unreg_guid, B.unreg_guid, C.unreg_guid),
                                  coalesce(A.event_id, B.event_id, C.event_id)
                     order by coalesce(A.device_id, B.device_id, C.device_id) desc,
                              coalesce(A.experience_level1, B.experience_level1, C.experience_level1),
                              coalesce(A.experience_level2, B.experience_level2, C.experience_level2),
                              coalesce(A.flex_column_3, B.flex_column_3, C.flex_column_3),
                              coalesce(A.flex_column_1, B.flex_column_1, C.flex_column_1)
   ) as row_numb,  
   coalesce(A.unreg_session_start_dt,B.unreg_session_start_dt,C.unreg_session_start_dt)  as dt,
   coalesce(A.event_dt,B.event_dt,C.event_dt)  as mktng_dt


   FROM             did_join                 A
   FULL OUTER JOIN  cguid_join               B
   ON               A.event_id            =  B.event_id
   AND              A.unreg_guid          =  B.unreg_guid
   AND              A.unreg_session_skey  =  B.unreg_session_skey
   FULL OUTER JOIN  xid_join                 C
   ON               A.event_id            =  C.event_id
   AND              A.unreg_guid          =  C.unreg_guid
   AND              A.unreg_session_skey  =  C.unreg_session_skey

   """ 
   
   val full_join_df = spark.sql(full_join_sql).filter($"row_numb"===1)
   //================================================================
   //
   //JOIN TO BBOWA EVENTS
   //
   //================================================================
   full_join_df.createOrReplaceTempView("full_join")
   //
   //
   
   val sess_join_bbowa_sql = """
   select 
   A.*,   
   B.user_id     as bbowa_user,
   B.num_watch,
   B.num_ask,
   B.num_ac,
   B.num_bo,
   B.num_bb,
   B.num_bbowa

   from full_join A
   left join bbowa_grouped B
   on   A.parent_uid  = B.user_id
   """

   val sess_join_bbowa_df = spark.sql(sess_join_bbowa_sql)

   //
   //
   (sess_join_bbowa_df
   .write
   .partitionBy("dt", "mktng_dt")
   .mode(SaveMode.Append)
   .save("hdfs://apollo-phx-nn-ha/user/hive/warehouse/mktng.db/sessions_signedin_mktng_corr")
  )
 }
}
