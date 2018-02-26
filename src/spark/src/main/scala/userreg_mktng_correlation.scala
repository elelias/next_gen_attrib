import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel


object UserRegCorrelationApp {

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



   val mktng_path = s"hdfs://apollo-phx-nn-ha/user/hive/warehouse/mktng.db/marketing_events_parquet"
   val user_reg_path = "hdfs://apollo-phx-nn-ha/user/hive/warehouse/mktng.db/user_regristration_xid_parquet"


   //LOAD FRAMES
   val mevents_df = spark.read.load(mktng_path)
   val reg_xid_df = spark.read.load(user_reg_path)


   //FILTER FOR DATES
   val reg_df   = reg_xid_df.filter(s"dt between '$reg_start_dt' and '$reg_end_dt'")
   val mktng_df = mevents_df.filter(s"dt between '$mktng_start_dt' and '$mktng_end_dt'")
   reg_df.cache()
   reg_df.createOrReplaceTempView("reg_xid")
   mktng_df.createOrReplaceTempView("mktng")




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
   B.user_id         as reg_user_id,
   B.user_cre_dt     as reg_user_cre_dt,
   B.user_cre_ts     as reg_user_cre_ts,
   B.user_cntry_id   as reg_user_cntry_id,
   B.guid            as reg_guid,
   B.cguid           as reg_cguid,
   B.session_skey    as reg_session_skey,
   B.session_start_dt as reg_session_start_dt,
   B.event_timestamp  as reg_event_timestamp,
   B.cobrand          as reg_cobrand,
   B.site_id          as reg_site_id,
   B.seqnum           as reg_seqnum,
   B.succ_reg_page_id as reg_succ_reg_page_id,
   B.session_traffic_source_id as reg_session_traffic_source_id,
   B.cust_traffic_source_level1 as reg_cust_traffic_source_level1,
   B.cust_traffic_source_level2 as reg_cust_traffic_source_level2,
   B.traffic_source_level3 as reg_traffic_source_level3,
   B.traffic_source_level4 as reg_traffic_source_level4,
   B.rotation_id           as reg_rotation_id,
   B.rvr_id                as reg_rvr_id,
   B.idfa                  as reg_idfa,
   B.gadid                 as reg_gadid,
   B.gdid                  as reg_gdid,
   B.device_type           as reg_device_type,
   B.device_type_level1    as reg_device_type_level1,
   B.device_type_level2    as reg_device_type_level2,
   B.experience_level1     as reg_experience_level1,
   B.experience_level2     as reg_experience_level2,
   B.vi_cnt                as reg_vi_cnt,
   B.incdata_id            as reg_incdata_id,
   (unix_timestamp(B.event_timestamp) - unix_timestamp(A.event_ts))/86400. as reg_day_diff,
   (unix_timestamp(B.event_timestamp) - unix_timestamp(A.event_ts)) as sec_day_diff,   
   """

   val cguid_query_tail = """
   "cguid"                 as join_strategy

   FROM        mktng      A
   INNER JOIN  reg_xid    B
   on          A.CGUID = B.CGUID

   where (A.event_dt   <= B.user_cre_dt)
   and  (datediff(B.user_cre_dt, A.event_dt) <= 14)
   and  (unix_timestamp(A.event_ts) <= unix_timestamp(B.user_cre_ts) + 600)
   and  (unix_timestamp(B.user_cre_ts) - unix_timestamp(A.event_ts) <= 14*86400)
   """

   val did_query_tail = """
   "did"                 as join_strategy

     FROM        mktng_non_null_did      A
   INNER JOIN  reg_xid_non_null_did    B
   on          A.device_id = B.device_id
   
   where (A.event_dt   <= B.user_cre_dt)
   and  (datediff(B.user_cre_dt, A.event_dt) <= 14)
   and  (unix_timestamp(A.event_ts) <= unix_timestamp(B.user_cre_ts) + 600)
   and  (unix_timestamp(B.user_cre_ts) - unix_timestamp(A.event_ts) <= 14*86400)
   """

   val xid_query_tail = """
   "xid"                 as join_strategy
   
   FROM        mktng_non_null_xid      A
   INNER JOIN  reg_xid_non_null_xid    B
   on          A.incdata_id = B.incdata_id
   
   where (A.event_dt   <= B.user_cre_dt)
   and  (datediff(B.user_cre_dt, A.event_dt) <= 14)
   and  (unix_timestamp(A.event_ts) <= unix_timestamp(B.user_cre_ts) + 600)
   and  (unix_timestamp(B.user_cre_ts) - unix_timestamp(A.event_ts) <= 14*86400)
   """

   sql("""select * from mktng where LENGTH(device_id)>=35 and device_id not like "%00000000-0000-0000-0000-000000000000%" """).createOrReplaceTempView("mktng_non_null_did")

   sql("select * from mktng where incdata_id is not null").createOrReplaceTempView("mktng_non_null_xid")

   sql("""select * from reg_xid where LENGTH(device_id)>=35 and device_id not like "%00000000-0000-0000-0000-000000000000%" """).createOrReplaceTempView("reg_xid_non_null_did")

   sql("select * from reg_xid where incdata_id is not null").createOrReplaceTempView("reg_xid_non_null_xid")

   sql(common_join_sql+cguid_query_tail).createOrReplaceTempView("cguid_join")
   sql(common_join_sql+did_query_tail).createOrReplaceTempView("did_join")
   sql(common_join_sql+did_query_tail).createOrReplaceTempView("xid_join")






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
   coalesce(A.reg_user_id,B.reg_user_id,C.reg_user_id) as reg_user_id,
   coalesce(A.reg_user_cre_dt,B.reg_user_cre_dt,C.reg_user_cre_dt) as reg_user_cre_dt,
   coalesce(A.reg_user_cre_ts,B.reg_user_cre_ts,C.reg_user_cre_ts) as reg_user_cre_ts,
   coalesce(A.reg_user_cntry_id,B.reg_user_cntry_id,C.reg_user_cntry_id) as reg_user_cntry_id,
   coalesce(A.reg_guid,B.reg_guid,C.reg_guid) as reg_guid,
   coalesce(A.reg_cguid,B.reg_cguid,C.reg_cguid) as reg_cguid,
   coalesce(A.reg_session_skey,B.reg_session_skey,C.reg_session_skey) as reg_session_skey,
   coalesce(A.reg_session_start_dt,B.reg_session_start_dt,C.reg_session_start_dt) as reg_session_start_dt,
   coalesce(A.reg_event_timestamp,B.reg_event_timestamp,C.reg_event_timestamp) as reg_event_timestamp,
   coalesce(A.reg_cobrand,B.reg_cobrand,C.reg_cobrand) as reg_cobrand,
   coalesce(A.reg_site_id,B.reg_site_id,C.reg_site_id) as reg_site_id,
   coalesce(A.reg_seqnum,B.reg_seqnum,C.reg_seqnum) as reg_seqnum,
   coalesce(A.reg_succ_reg_page_id,B.reg_succ_reg_page_id,C.reg_succ_reg_page_id) as reg_succ_reg_page_id,
   coalesce(A.reg_session_traffic_source_id,B.reg_session_traffic_source_id,C.reg_session_traffic_source_id) as reg_session_traffic_source_id,
   coalesce(A.reg_cust_traffic_source_level1,B.reg_cust_traffic_source_level1,C.reg_cust_traffic_source_level1) as reg_cust_traffic_source_level1,
   coalesce(A.reg_cust_traffic_source_level2,B.reg_cust_traffic_source_level2,C.reg_cust_traffic_source_level2) as reg_cust_traffic_source_level2,
   coalesce(A.reg_traffic_source_level3,B.reg_traffic_source_level3,C.reg_traffic_source_level3) as reg_traffic_source_level3,
   coalesce(A.reg_traffic_source_level4,B.reg_traffic_source_level4,C.reg_traffic_source_level4) as reg_traffic_source_level4,
   coalesce(A.reg_rotation_id,B.reg_rotation_id,C.reg_rotation_id) as reg_rotation_id,
   coalesce(A.reg_rvr_id,B.reg_rvr_id,C.reg_rvr_id) as reg_rvr_id,
   coalesce(A.reg_idfa,B.reg_idfa,C.reg_idfa) as reg_idfa,
   coalesce(A.reg_gadid,B.reg_gadid,C.reg_gadid) as reg_gadid,
   coalesce(A.reg_gdid,B.reg_gdid,C.reg_gdid) as reg_gdid,
   coalesce(A.reg_device_type,B.reg_device_type,C.reg_device_type) as reg_device_type,
   coalesce(A.reg_device_type_level1,B.reg_device_type_level1,C.reg_device_type_level1) as reg_device_type_level1,
   coalesce(A.reg_device_type_level2,B.reg_device_type_level2,C.reg_device_type_level2) as reg_device_type_level2,
   coalesce(A.reg_experience_level1,B.reg_experience_level1,C.reg_experience_level1) as reg_experience_level1,
   coalesce(A.reg_experience_level2,B.reg_experience_level2,C.reg_experience_level2) as reg_experience_level2,
   coalesce(A.reg_vi_cnt,B.reg_vi_cnt,C.reg_vi_cnt)   as reg_vi_cnt,
   coalesce(A.reg_incdata_id,B.reg_incdata_id,C.reg_incdata_id) as reg_incdata_id,
   coalesce(A.reg_day_diff,B.reg_day_diff,C.reg_day_diff)        as reg_day_diff,
   coalesce(A.sec_day_diff,B.sec_day_diff,C.sec_day_diff)  as sec_day_diff,
   coalesce(A.join_strategy, B.join_strategy, C.join_strategy) as join_strategy,
   coalesce(A.reg_user_cre_dt,B.reg_user_cre_dt,C.reg_user_cre_dt)  as mktng_dt

   
   FROM             did_join        A
   FULL OUTER JOIN  cguid_join      B
   ON               A.event_id    = B.event_id
   AND              A.reg_user_id = B.reg_user_id
   
   FULL OUTER JOIN  xid_join        C
   ON               B.event_id    = C.event_id
   and              B.reg_user_id = C.reg_user_id
   """ 
   

   val full_join_df = spark.sql(full_join_sql)


   full_join_df
   .write
   .partitionBy("mktng_dt")
   .mode(SaveMode.Append)
   .save("hdfs://apollo-phx-nn-ha/user/hive/warehouse/mktng.db/registration_mktng_correlation")

 }
}
