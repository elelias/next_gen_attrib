
/* SimpleApp.scala */
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel


object CorrelationApp {

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
   
   args.foreach(println)
   println("HELLO")

   val cko_start_dt   = args(0)
   val cko_end_dt     = args(1)
   val data_source    = args(2)
   val mktng_start_dt = args(3)
   val mktng_end_dt   = cko_end_dt
   //val mktng_end_dt   = args(4)
   
   
   val txns_path =  s"hdfs://apollo-phx-nn-ha/user/hive/warehouse/txns.db/txn_events_with_bbowa_incdata"

   val mktng_path = s"hdfs://apollo-phx-nn-ha/user/hive/warehouse/mktng.db/marketing_events_parquet/data_source=$data_source"
      

   val txns_df = spark.read.load(txns_path).filter(s"dt between '$cko_start_dt' and '$cko_end_dt'")
   
   txns_df.cache()
   //val txns_count = txns_df.count()
   //println("there are "+txns_count+" transactions")

   txns_df.createOrReplaceTempView("txns")

   val mktng_df  = spark.read.load(mktng_path).filter(s"dt between '$mktng_start_dt' and '$mktng_end_dt'")
   //mktng_df.persist(StorageLevel.MEMORY_AND_DISK)
   mktng_df.createOrReplaceTempView("mktng")



   
 
   val common_join_query = s"""
   select   /*+ BROADCAST (B) */
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
   B.item_id,
   B.transaction_id,
   B.auct_end_dt,
   B.item_site_id,
   B.auct_type_code,
   B.leaf_categ_id,
   B.seller_id,
   B.seller_country_id,
   B.buyer_id,
   B.buyer_country_id,
   B.fm_buyer_type_cd,
   B.fm_buyer_type_desc,
   B.is_first_purchase,
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
   B.bbowa_device_type,
   B.bbowa_experience_level1,
   B.bbowa_experience_level2,
   B.bbowa_device_type_level1,
   B.bbowa_device_type_level2,
   B.bbowa_traffic_source_id,
   B.sess_device_type,
   B.sess_exp_level1 as sess_experience_level1,
   B.sess_exp_level2 as sess_experience_level2,
   B.sess_device_type_level1,
   B.sess_device_type_level2,
   B.gaid,
   B.idfa,
   B.device_id as cko_device_id,
   B.incdata_id as cko_incdata_id,
   B.incdata_dqscore as cko_incdata_dqscore,
   'uid' as join_strategy,
   (unix_timestamp(bbowa_event_timestamp) - unix_timestamp(event_ts))/86400. as bbowa_day_diff,
    unix_timestamp(bbowa_event_timestamp) - unix_timestamp(event_ts)         as bbowa_sec_diff,
   (unix_timestamp(created_time) - unix_timestamp(event_ts))/86400.          as cko_day_diff,
    unix_timestamp(created_time) - unix_timestamp(event_ts)                  as cko_sec_diff,
   case when (unix_timestamp(bbowa_event_timestamp) - unix_timestamp(event_ts)) > -600 then 1 else 0 end as is_before_bbowa,
   case when (unix_timestamp(created_time) - unix_timestamp(event_ts)) > -600 then 1 else 0 end            as is_before_checkout,
   
   -1 as is_same_device,
   -1 as is_same_exp
   """






   //================================
   //UID
   //================================
   val mktng_non_null_uid_df = mktng_df.filter($"user_id" >0)   
   mktng_non_null_uid_df.createOrReplaceTempView("uid_mktng")

   val uid_join_from_query = """
   FROM		txns B
   inner join 	uid_mktng A
   on    A.user_id = B.buyer_id

   where (A.event_dt <= B.created_dt)
   and   (datediff(B.created_dt, A.event_dt) <= 14+10+1)
   and   (unix_timestamp(A.event_ts) <= unix_timestamp(B.created_time) + 600)
   and   (unix_timestamp(B.created_time) - unix_timestamp(A.event_ts) <= (14+10)*86400)
   """

   val uid_join_query = common_join_query + uid_join_from_query
   val uid_join_df = sql(uid_join_query)
   uid_join_df.createOrReplaceTempView("uid_join")
   
   //uid_join_df.cache()
   //val uid_join_count = uid_join_df.count()
   //println("uid count "+uid_join_count)
   //================================






   //================================
   //DEVICEID
   //================================
   val q1 = """
   select * from txns
   where LENGTH(device_id)>=35
   and device_id not like "%00000000-0000-0000-0000-000000000000%"
   """
   val txns_non_null_deviceid_df = sql(q1)
   txns_non_null_deviceid_df.createOrReplaceTempView("txns_deviceid")

   val q2 = """
   select * from mktng
   where LENGTH(device_id)>=35
   and device_id not like "%00000000-0000-0000-0000-000000000000%"
   """ 
   val mktng_non_null_deviceid_df = sql(q2)
   mktng_non_null_deviceid_df.createOrReplaceTempView("mktng_deviceid")

   //
   //
   //
   val deviceid_join_from_query = """
   FROM        mktng_deviceid      A
   INNER JOIN  txns_deviceid       B
   on          A.device_id = B.device_id
   where (A.event_dt <= B.created_dt)
   and   (datediff(B.created_dt, A.event_dt) <= 14+10+1)
   and   (unix_timestamp(A.event_ts) <= unix_timestamp(B.created_time) + 600)
   and   (unix_timestamp(B.created_time) - unix_timestamp(A.event_ts) <= (14+10)*86400)   
   """
   val deviceid_join_query = common_join_query + deviceid_join_from_query 
   val deviceid_join_df    = sql(deviceid_join_query)
   deviceid_join_df.createOrReplaceTempView("deviceid_join")
   //================================





   //================================
   //CGUID
   //================================
   val txns_non_null_cguid_df  = txns_df.filter("bbowa_cguid is not null")
   txns_non_null_cguid_df.createOrReplaceTempView("txns_cguid")
   //
   val cguid_join_from_query = """
   FROM        mktng      A
   INNER JOIN  txns_cguid B
   on          A.CGUID = B.BBOWA_CGUID

   where (A.event_dt <= B.created_dt)
   and  (datediff(B.created_dt, A.event_dt) <= 14+10+1)
   and  (unix_timestamp(A.event_ts) <= unix_timestamp(B.created_time) + 600)
   and  (unix_timestamp(B.created_time) - unix_timestamp(A.event_ts) <= (14+10)*86400)
   """
   val cguid_join_query = common_join_query + cguid_join_from_query 
   val cguid_join_df    = sql(cguid_join_query)

   //cguid_join_df.cache()
   //val cguid_join_count = cguid_join_df.count()
   //println("cguid_join_count = "+cguid_join_count)

   cguid_join_df.createOrReplaceTempView("cguid_join")
   //================================





   //================================
   //XID
   //================================
   val txns_non_null_xid_df  = txns_df.filter("incdata_id is not null")
   txns_non_null_xid_df.createOrReplaceTempView("txns_xid")
   //
   val mktng_non_null_xid_df  = mktng_df.filter("incdata_id is not null")
   mktng_non_null_xid_df.createOrReplaceTempView("mktng_xid")
   //
   val xid_join_from_query = """
   FROM        mktng_xid      A
   INNER JOIN  txns_xid       B
   on          A.INCDATA_ID = B.INCDATA_ID

   where (A.event_dt <= B.created_dt)
   and   (datediff(B.created_dt, A.event_dt) <= 14+10+1)
   and   (unix_timestamp(A.event_ts) <= unix_timestamp(B.created_time) + 600)
   and   (unix_timestamp(B.created_time) - unix_timestamp(A.event_ts) <= (14+10) *86400)
   """
   val xid_join_query = common_join_query + xid_join_from_query 
   val xid_join_df    = sql(xid_join_query)
   val xid_join_table = xid_join_df.createOrReplaceTempView("xid_join")
   //================================




   val uid_deviceid_join_query = """
   select 
   distinct
   coalesce(A.guid, B.guid) as guid,
   coalesce(A.cguid, B.cguid) as cguid,
   coalesce(A.user_id, B.user_id) as user_id,
   coalesce(A.event_id, B.event_id) as event_id,
   coalesce(A.event_id_type, B.event_id_type) as event_id_type,
   coalesce(A.event_type_id, B.event_type_id) as event_type_id,
   coalesce(A.channel_id, B.channel_id) as channel_id,
   coalesce(A.channel_name, B.channel_name) as channel_name,
   coalesce(A.rotation_id, B.rotation_id) as rotation_id,
   coalesce(A.rotation_name, B.rotation_name) as rotation_name,
   coalesce(A.campaign_id, B.campaign_id) as campaign_id,
   coalesce(A.campaign_name, B.campaign_name) as campaign_name,
   coalesce(A.event_dt, B.event_dt) as event_dt,
   coalesce(A.event_ts, B.event_ts) as event_ts,
   coalesce(A.incdata_id, B.incdata_id) as incdata_id,
   coalesce(A.cluster_id, B.cluster_id) as cluster_id,
   coalesce(A.device_id, B.device_id) as device_id,
   coalesce(A.experience_level1, B.experience_level1) as experience_level1,
   coalesce(A.experience_level2, B.experience_level2) as experience_level2,
   coalesce(A.device_type_level1, B.device_type_level1) as device_type_level1,
   coalesce(A.device_type_level2, B.device_type_level2) as device_type_level2,
   coalesce(A.flex_column_1, B.flex_column_1) as flex_column_1,
   coalesce(A.flex_column_2, B.flex_column_2) as flex_column_2,
   coalesce(A.flex_column_3, B.flex_column_3) as flex_column_3,
   coalesce(A.flex_column_4, B.flex_column_4) as flex_column_4,
   coalesce(A.flex_column_5, B.flex_column_5) as flex_column_5,
   coalesce(A.flex_column_6, B.flex_column_6) as flex_column_6,
   
   coalesce(A.item_id, B.item_id) as item_id,
   coalesce(A.transaction_id, B.transaction_id) as transaction_id,
   coalesce(A.auct_end_dt, B.auct_end_dt) as auct_end_dt,
   coalesce(A.item_site_id, B.item_site_id) as item_site_id,
   coalesce(A.auct_type_code, B.auct_type_code) as auct_type_code,
   coalesce(A.leaf_categ_id, B.leaf_categ_id) as leaf_categ_id,

   coalesce(A.seller_id, B.seller_id) as seller_id,
   coalesce(A.seller_country_id, B.seller_country_id) as seller_country_id,
   
   coalesce(A.buyer_id, B.buyer_id) as buyer_id,
   coalesce(A.buyer_country_id, B.buyer_country_id) as buyer_country_id,
   coalesce(A.fm_buyer_type_cd, B.fm_buyer_type_cd) as fm_buyer_type_cd,
   coalesce(A.fm_buyer_type_desc, B.fm_buyer_type_desc) as fm_buyer_type_desc,
   coalesce(A.is_first_purchase, B.is_first_purchase) as is_first_purchase,
   
   coalesce(A.created_dt, B.created_dt) as created_dt,
   coalesce(A.created_time, B.created_time) as created_time,
   coalesce(A.gmb_usd, B.gmb_usd) as gmb_usd,

   coalesce(A.sess_guid, B.sess_guid) as sess_guid,
   coalesce(A.sess_cguid, B.sess_cguid) as sess_cguid,
   coalesce(A.sess_session_skey, B.sess_session_skey) as sess_session_skey,
   coalesce(A.sess_session_start_dt, B.sess_session_start_dt) as sess_session_start_dt,
   coalesce(A.sess_site_id, B.sess_site_id) as sess_site_id,
   coalesce(A.sess_cobrand, B.sess_cobrand) as sess_cobrand,

   coalesce(A.bbowa_guid, B.bbowa_guid) as bbowa_guid,
   coalesce(A.bbowa_cguid, B.bbowa_cguid) as bbowa_cguid,
   coalesce(A.bbowa_session_skey, B.bbowa_session_skey) as bbowa_session_skey,
   coalesce(A.bbowa_session_start_dt, B.bbowa_session_start_dt) as bbowa_session_start_dt,
   coalesce(A.bbowa_event_timestamp, B.bbowa_event_timestamp) as bbowa_event_timestamp,
   coalesce(A.bbowa_site_id, B.bbowa_site_id) as bbowa_site_id,
   coalesce(A.bbowa_cobrand, B.bbowa_cobrand) as bbowa_cobrand,
   coalesce(A.bbowa_device_type, B.bbowa_device_type) as bbowa_device_type,
   coalesce(A.bbowa_experience_level1, B.bbowa_experience_level1) as bbowa_experience_level1,
   coalesce(A.bbowa_experience_level2, B.bbowa_experience_level2) as bbowa_experience_level2,
   coalesce(A.bbowa_device_type_level1, B.bbowa_device_type_level1) as bbowa_device_type_level1,
   coalesce(A.bbowa_device_type_level2, B.bbowa_device_type_level2) as bbowa_device_type_level2,
   coalesce(A.bbowa_traffic_source_id, B.bbowa_traffic_source_id) as bbowa_traffic_source_id,

   coalesce(A.sess_device_type, B.sess_device_type) as sess_device_type,
   coalesce(A.sess_experience_level1, B.sess_experience_level1) as sess_experience_level1,
   coalesce(A.sess_experience_level2, B.sess_experience_level2) as sess_experience_level2,
   coalesce(A.sess_device_type_level1, B.sess_device_type_level1) as sess_device_type_level1,
   coalesce(A.sess_device_type_level2, B.sess_device_type_level2) as sess_device_type_level2,
   
   coalesce(A.gaid, B.gaid) as gaid,
   coalesce(A.idfa, B.idfa) as idfa,
   coalesce(A.cko_device_id, B.cko_device_id) as cko_device_id,
   coalesce(A.cko_incdata_id, B.cko_incdata_id) as cko_incdata_id,
   coalesce(A.cko_incdata_dqscore, B.cko_incdata_dqscore) as cko_incdata_dqscore,

   coalesce(A.join_strategy, B.join_strategy) as join_strategy,
   coalesce(A.bbowa_day_diff, B.bbowa_day_diff) as bbowa_day_diff,
   coalesce(A.bbowa_sec_diff, B.bbowa_sec_diff) as bbowa_sec_diff,
   coalesce(A.cko_day_diff, B.cko_day_diff) as cko_day_diff,
   coalesce(A.cko_sec_diff, B.cko_sec_diff) as cko_sec_diff,

   coalesce(A.is_same_device, B.is_same_device) as is_same_device,
   coalesce(A.is_same_exp, B.is_same_exp) as is_same_exp



   FROM 			uid_join           A
   FULL OUTER JOIN		deviceid_join      B
   ON   	   		A.event_id       = B.event_id
   AND			        A.item_id        = B.item_id
   AND			        A.transaction_id = B.transaction_id   
   """

   val uid_deviceid_join_df = sql(uid_deviceid_join_query)
   uid_deviceid_join_df.createOrReplaceTempView("uid_deviceid_join")

   //uid_deviceid_join_df.cache()
   //val uid_deviceid_join_count = uid_deviceid_join_df.count()
   //println("joined uid_device_id = "+uid_deviceid_join_count)



   val cguid_xid_join_query = """
   select 
   distinct
   coalesce(A.guid, B.guid) as guid,
   coalesce(A.cguid, B.cguid) as cguid,
   coalesce(A.user_id, B.user_id) as user_id,
   coalesce(A.event_id, B.event_id) as event_id,
   coalesce(A.event_id_type, B.event_id_type) as event_id_type,
   coalesce(A.event_type_id, B.event_type_id) as event_type_id,
   coalesce(A.channel_id, B.channel_id) as channel_id,
   coalesce(A.channel_name, B.channel_name) as channel_name,
   coalesce(A.rotation_id, B.rotation_id) as rotation_id,
   coalesce(A.rotation_name, B.rotation_name) as rotation_name,
   coalesce(A.campaign_id, B.campaign_id) as campaign_id,
   coalesce(A.campaign_name, B.campaign_name) as campaign_name,
   coalesce(A.event_dt, B.event_dt) as event_dt,
   coalesce(A.event_ts, B.event_ts) as event_ts,
   coalesce(A.incdata_id, B.incdata_id) as incdata_id,
   coalesce(A.cluster_id, B.cluster_id) as cluster_id,
   coalesce(A.device_id, B.device_id) as device_id,
   coalesce(A.experience_level1, B.experience_level1) as experience_level1,
   coalesce(A.experience_level2, B.experience_level2) as experience_level2,
   coalesce(A.device_type_level1, B.device_type_level1) as device_type_level1,
   coalesce(A.device_type_level2, B.device_type_level2) as device_type_level2,
   coalesce(A.flex_column_1, B.flex_column_1) as flex_column_1,
   coalesce(A.flex_column_2, B.flex_column_2) as flex_column_2,
   coalesce(A.flex_column_3, B.flex_column_3) as flex_column_3,
   coalesce(A.flex_column_4, B.flex_column_4) as flex_column_4,
   coalesce(A.flex_column_5, B.flex_column_5) as flex_column_5,
   coalesce(A.flex_column_6, B.flex_column_6) as flex_column_6,
   
   coalesce(A.item_id, B.item_id) as item_id,
   coalesce(A.transaction_id, B.transaction_id) as transaction_id,
   coalesce(A.auct_end_dt, B.auct_end_dt) as auct_end_dt,
   coalesce(A.item_site_id, B.item_site_id) as item_site_id,
   coalesce(A.auct_type_code, B.auct_type_code) as auct_type_code,
   coalesce(A.leaf_categ_id, B.leaf_categ_id) as leaf_categ_id,

   coalesce(A.seller_id, B.seller_id) as seller_id,
   coalesce(A.seller_country_id, B.seller_country_id) as seller_country_id,
   
   coalesce(A.buyer_id, B.buyer_id) as buyer_id,
   coalesce(A.buyer_country_id, B.buyer_country_id) as buyer_country_id,
   coalesce(A.fm_buyer_type_cd, B.fm_buyer_type_cd) as fm_buyer_type_cd,
   coalesce(A.fm_buyer_type_desc, B.fm_buyer_type_desc) as fm_buyer_type_desc,
   coalesce(A.is_first_purchase, B.is_first_purchase) as is_first_purchase,
   
   coalesce(A.created_dt, B.created_dt) as created_dt,
   coalesce(A.created_time, B.created_time) as created_time,
   coalesce(A.gmb_usd, B.gmb_usd) as gmb_usd,

   coalesce(A.sess_guid, B.sess_guid) as sess_guid,
   coalesce(A.sess_cguid, B.sess_cguid) as sess_cguid,
   coalesce(A.sess_session_skey, B.sess_session_skey) as sess_session_skey,
   coalesce(A.sess_session_start_dt, B.sess_session_start_dt) as sess_session_start_dt,
   coalesce(A.sess_site_id, B.sess_site_id) as sess_site_id,
   coalesce(A.sess_cobrand, B.sess_cobrand) as sess_cobrand,

   coalesce(A.bbowa_guid, B.bbowa_guid) as bbowa_guid,
   coalesce(A.bbowa_cguid, B.bbowa_cguid) as bbowa_cguid,
   coalesce(A.bbowa_session_skey, B.bbowa_session_skey) as bbowa_session_skey,
   coalesce(A.bbowa_session_start_dt, B.bbowa_session_start_dt) as bbowa_session_start_dt,
   coalesce(A.bbowa_event_timestamp, B.bbowa_event_timestamp) as bbowa_event_timestamp,
   coalesce(A.bbowa_site_id, B.bbowa_site_id) as bbowa_site_id,
   coalesce(A.bbowa_cobrand, B.bbowa_cobrand) as bbowa_cobrand,
   coalesce(A.bbowa_device_type, B.bbowa_device_type) as bbowa_device_type,
   coalesce(A.bbowa_experience_level1, B.bbowa_experience_level1) as bbowa_experience_level1,
   coalesce(A.bbowa_experience_level2, B.bbowa_experience_level2) as bbowa_experience_level2,
   coalesce(A.bbowa_device_type_level1, B.bbowa_device_type_level1) as bbowa_device_type_level1,
   coalesce(A.bbowa_device_type_level2, B.bbowa_device_type_level2) as bbowa_device_type_level2,
   coalesce(A.bbowa_traffic_source_id, B.bbowa_traffic_source_id) as bbowa_traffic_source_id,

   coalesce(A.sess_device_type, B.sess_device_type) as sess_device_type,
   coalesce(A.sess_experience_level1, B.sess_experience_level1) as sess_experience_level1,
   coalesce(A.sess_experience_level2, B.sess_experience_level2) as sess_experience_level2,
   coalesce(A.sess_device_type_level1, B.sess_device_type_level1) as sess_device_type_level1,
   coalesce(A.sess_device_type_level2, B.sess_device_type_level2) as sess_device_type_level2,
   
   coalesce(A.gaid, B.gaid) as gaid,
   coalesce(A.idfa, B.idfa) as idfa,
   coalesce(A.cko_device_id, B.cko_device_id) as cko_device_id,
   coalesce(A.cko_incdata_id, B.cko_incdata_id) as cko_incdata_id,
   coalesce(A.cko_incdata_dqscore, B.cko_incdata_dqscore) as cko_incdata_dqscore,

   coalesce(A.join_strategy, B.join_strategy) as join_strategy,
   coalesce(A.bbowa_day_diff, B.bbowa_day_diff) as bbowa_day_diff,
   coalesce(A.bbowa_sec_diff, B.bbowa_sec_diff) as bbowa_sec_diff,
   coalesce(A.cko_day_diff,   B.cko_day_diff) as cko_day_diff,
   coalesce(A.cko_sec_diff,   B.cko_sec_diff) as cko_sec_diff,

   coalesce(A.is_same_device, B.is_same_device) as is_same_device,
   coalesce(A.is_same_exp, B.is_same_exp) as is_same_exp



   FROM 			cguid_join         A
   FULL OUTER JOIN		xid_join           B
   ON   	   		A.event_id       = B.event_id
   AND			        A.item_id        = B.item_id
   AND			        A.transaction_id = B.transaction_id   
   """

   val cguid_xid_join_df = sql(cguid_xid_join_query)
   cguid_xid_join_df.createOrReplaceTempView("cguid_xid_join")
   




   val merge_all_join_query = s"""
   select 
   distinct
   coalesce(A.guid, B.guid) as guid,
   coalesce(A.cguid, B.cguid) as cguid,
   coalesce(A.user_id, B.user_id) as user_id,
   coalesce(A.event_id, B.event_id) as event_id,
   coalesce(A.event_id_type, B.event_id_type) as event_id_type,
   coalesce(A.event_type_id, B.event_type_id) as event_type_id,
   coalesce(A.channel_id, B.channel_id) as channel_id,
   coalesce(A.channel_name, B.channel_name) as channel_name,
   coalesce(A.rotation_id, B.rotation_id) as rotation_id,
   coalesce(A.rotation_name, B.rotation_name) as rotation_name,
   coalesce(A.campaign_id, B.campaign_id) as campaign_id,
   coalesce(A.campaign_name, B.campaign_name) as campaign_name,
   coalesce(A.event_dt, B.event_dt) as event_dt,
   coalesce(A.event_ts, B.event_ts) as event_ts,
   coalesce(A.incdata_id, B.incdata_id) as incdata_id,
   coalesce(A.cluster_id, B.cluster_id) as cluster_id,
   coalesce(A.device_id, B.device_id) as device_id,
   coalesce(A.experience_level1, B.experience_level1) as experience_level1,
   coalesce(A.experience_level2, B.experience_level2) as experience_level2,
   coalesce(A.device_type_level1, B.device_type_level1) as device_type_level1,
   coalesce(A.device_type_level2, B.device_type_level2) as device_type_level2,
   coalesce(A.flex_column_1, B.flex_column_1) as flex_column_1,
   coalesce(A.flex_column_2, B.flex_column_2) as flex_column_2,
   coalesce(A.flex_column_3, B.flex_column_3) as flex_column_3,
   coalesce(A.flex_column_4, B.flex_column_4) as flex_column_4,
   coalesce(A.flex_column_5, B.flex_column_5) as flex_column_5,
   coalesce(A.flex_column_6, B.flex_column_6) as flex_column_6,
   
   coalesce(A.item_id, B.item_id) as item_id,
   coalesce(A.transaction_id, B.transaction_id) as transaction_id,
   coalesce(A.auct_end_dt, B.auct_end_dt) as auct_end_dt,
   coalesce(A.item_site_id, B.item_site_id) as item_site_id,
   coalesce(A.auct_type_code, B.auct_type_code) as auct_type_code,
   coalesce(A.leaf_categ_id, B.leaf_categ_id) as leaf_categ_id,

   coalesce(A.seller_id, B.seller_id) as seller_id,
   coalesce(A.seller_country_id, B.seller_country_id) as seller_country_id,
   
   coalesce(A.buyer_id, B.buyer_id) as buyer_id,
   coalesce(A.buyer_country_id, B.buyer_country_id) as buyer_country_id,
   coalesce(A.fm_buyer_type_cd, B.fm_buyer_type_cd) as fm_buyer_type_cd,
   coalesce(A.fm_buyer_type_desc, B.fm_buyer_type_desc) as fm_buyer_type_desc,
   coalesce(A.is_first_purchase, B.is_first_purchase) as is_first_purchase,
   
   coalesce(A.created_dt, B.created_dt) as created_dt,
   coalesce(A.created_time, B.created_time) as created_time,
   coalesce(A.gmb_usd, B.gmb_usd) as gmb_usd,

   coalesce(A.sess_guid, B.sess_guid) as sess_guid,
   coalesce(A.sess_cguid, B.sess_cguid) as sess_cguid,
   coalesce(A.sess_session_skey, B.sess_session_skey) as sess_session_skey,
   coalesce(A.sess_session_start_dt, B.sess_session_start_dt) as sess_session_start_dt,
   coalesce(A.sess_site_id, B.sess_site_id) as sess_site_id,
   coalesce(A.sess_cobrand, B.sess_cobrand) as sess_cobrand,

   coalesce(A.bbowa_guid, B.bbowa_guid) as bbowa_guid,
   coalesce(A.bbowa_cguid, B.bbowa_cguid) as bbowa_cguid,
   coalesce(A.bbowa_session_skey, B.bbowa_session_skey) as bbowa_session_skey,
   coalesce(A.bbowa_session_start_dt, B.bbowa_session_start_dt) as bbowa_session_start_dt,
   coalesce(A.bbowa_event_timestamp, B.bbowa_event_timestamp) as bbowa_event_timestamp,
   coalesce(A.bbowa_site_id, B.bbowa_site_id) as bbowa_site_id,
   coalesce(A.bbowa_cobrand, B.bbowa_cobrand) as bbowa_cobrand,
   coalesce(A.bbowa_device_type, B.bbowa_device_type) as bbowa_device_type,
   coalesce(A.bbowa_experience_level1, B.bbowa_experience_level1) as bbowa_experience_level1,
   coalesce(A.bbowa_experience_level2, B.bbowa_experience_level2) as bbowa_experience_level2,
   coalesce(A.bbowa_device_type_level1, B.bbowa_device_type_level1) as bbowa_device_type_level1,
   coalesce(A.bbowa_device_type_level2, B.bbowa_device_type_level2) as bbowa_device_type_level2,
   coalesce(A.bbowa_traffic_source_id, B.bbowa_traffic_source_id) as bbowa_traffic_source_id,

   coalesce(A.sess_device_type, B.sess_device_type) as sess_device_type,
   coalesce(A.sess_experience_level1, B.sess_experience_level1) as sess_experience_level1,
   coalesce(A.sess_experience_level2, B.sess_experience_level2) as sess_experience_level2,
   coalesce(A.sess_device_type_level1, B.sess_device_type_level1) as sess_device_type_level1,
   coalesce(A.sess_device_type_level2, B.sess_device_type_level2) as sess_device_type_level2,
   
   coalesce(A.gaid, B.gaid) as gaid,
   coalesce(A.idfa, B.idfa) as idfa,
   coalesce(A.cko_device_id, B.cko_device_id) as cko_device_id,
   coalesce(A.cko_incdata_id, B.cko_incdata_id) as cko_incdata_id,
   coalesce(A.cko_incdata_dqscore, B.cko_incdata_dqscore) as cko_incdata_dqscore,
   coalesce(A.join_strategy, B.join_strategy) as join_strategy,

   coalesce(A.bbowa_day_diff, B.bbowa_day_diff) as bbowa_day_diff,
   coalesce(A.bbowa_sec_diff, B.bbowa_sec_diff) as bbowa_sec_diff,
   coalesce(A.cko_day_diff, B.cko_day_diff) as cko_day_diff,
   coalesce(A.cko_sec_diff, B.cko_sec_diff) as cko_sec_diff,

   coalesce(A.is_same_device, B.is_same_device) as is_same_device,
   coalesce(A.is_same_exp, B.is_same_exp) as is_same_exp,

   '$data_source'                         as data_source,
   coalesce(A.created_dt, B.created_dt)   as ck_dt,
   coalesce(A.event_dt, B.event_dt)       as mktng_dt


   FROM 			uid_deviceid_join         A
   FULL OUTER JOIN		cguid_xid_join            B
   ON   	   		A.event_id       = B.event_id
   AND			        A.item_id        = B.item_id
   AND			        A.transaction_id = B.transaction_id   
   """

   val merge_all_join_df = sql(merge_all_join_query)
   //val merge_count = merge_all_join_df.count()
   //println("merge_count  = " + merge_count)

   merge_all_join_df
   .repartition($"data_source",$"ck_dt",$"mktng_dt")
   .write
   .partitionBy("data_source","ck_dt","mktng_dt")
   .mode(SaveMode.Append)
   .save("hdfs://apollo-phx-nn-ha/user/hive/warehouse/mktng.db/me_txns_correlation")
   
  }
}
