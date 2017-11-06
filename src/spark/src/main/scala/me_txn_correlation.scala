
/* SimpleApp.scala */
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession


object CorrelationApp {

 def main(args: Array[String]) {
   

   val spark = SparkSession
   .builder
   .master("yarn")
   .appName("correlation")
   .getOrCreate()
   val sc    = spark.sparkContext
   sc.setLogLevel("ERROR")

   import spark.implicits._
   import spark.sql
   
   args.foreach(println)

   val cko_start_dt   = args(0)
   val cko_end_dt     = args(1)
   val data_source    = args(2)
   val mktng_start_dt = args(3)
   val mktng_end_dt   = cko_end_dt
   
   
   val txns_path =  "hdfs://apollo-phx-nn-ha/user/hive/warehouse/txns.db/txns_with_xid_parquet/"
   val mktng_path = "hdfs://apollo-phx-nn-ha/user/hive/warehouse/mktng.db/marketing_events_parquet/"
      



   val txns_df = spark.read.load(txns_path).filter(s"dt between '$cko_start_dt' and '$cko_end_dt'")
   txns_df.createOrReplaceTempView("txns")

   val mktng_df  = spark.read.load(mktng_path).filter(s"dt between '$mktng_start_dt' and '$mktng_end_dt' and data_source='$data_source'")
   mktng_df.createOrReplaceTempView("mktng")


 
   val common_join_query = """
   select
   distinct
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
   B.FM_BUYER_TYPE_CD,
   B.FM_BUYER_TYPE_DESC,
   B.IS_FIRST_PURCHASE,
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
   B.sess_experience_level1,
   B.sess_experience_level2,
   B.sess_device_type_level1,
   B.sess_device_type_level2,
   B.gaid,
   B.idfa,
   B.device_id as cko_device_id,
   B.incdata_id as cko_incdata_id,
   B.incdata_dqscore as cko_incdata_dqscore,
   'uid' as join_strategy,
   (unix_timestamp(bbowa_event_timestamp) - unix_timestamp(event_ts))/86400. as day_diff,
   unix_timestamp(bbowa_event_timestamp) - unix_timestamp(event_ts) as sec_diff,
   -1 as is_same_device,
   -1 as is_same_exp,
   '$data_source' as data_source
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

   where (A.event_dt <= B.bbowa_session_start_dt)
   and   (datediff(B.bbowa_session_start_dt, A.event_dt) <= 30)
   and   (unix_timestamp(A.event_ts) <= unix_timestamp(B.bbowa_event_timestamp) + 600)
   and   (unix_timestamp(B.bbowa_event_timestamp) - unix_timestamp(A.event_ts) <= 2592000)
   """

   val uid_join_query = common_join_query + uid_join_from_query
   val uid_join_df = sql(uid_join_query)
   uid_join_df.createOrReplaceTempView("uid_join")
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
   where (A.event_dt <= B.bbowa_session_start_dt)
   and   (datediff(B.bbowa_session_start_dt, A.event_dt) <= 30)
   and   (unix_timestamp(A.event_ts) <= unix_timestamp(B.bbowa_event_timestamp) + 600)
   and   (unix_timestamp(B.bbowa_event_timestamp) - unix_timestamp(A.event_ts) <= 2592000)   
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

   where (A.event_dt <= B.bbowa_session_start_dt)
   and  (datediff(B.bbowa_session_start_dt, A.event_dt) <= 30)
   and   (unix_timestamp(A.event_ts) <= unix_timestamp(B.bbowa_event_timestamp) + 600)
   and (unix_timestamp(B.bbowa_event_timestamp) - unix_timestamp(A.event_ts) <= 2592000)   
   """
   val cguid_join_query = common_join_query + cguid_join_from_query 
   val cguid_join_df    = sql(cguid_join_query)
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

   where (A.event_dt <= B.bbowa_session_start_dt)
   and  (datediff(B.bbowa_session_start_dt, A.event_dt) <= 30)
   and   (unix_timestamp(A.event_ts) <= unix_timestamp(B.bbowa_event_timestamp) + 600)
   and (unix_timestamp(B.bbowa_event_timestamp) - unix_timestamp(A.event_ts) <= 2592000)   
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
   coalesce(A.FM_BUYER_TYPE_CD, B.FM_BUYER_TYPE_CD) as FM_BUYER_TYPE_CD,
   coalesce(A.FM_BUYER_TYPE_DESC, B.FM_BUYER_TYPE_DESC) as FM_BUYER_TYPE_DESC,
   coalesce(A.IS_FIRST_PURCHASE, B.IS_FIRST_PURCHASE) as IS_FIRST_PURCHASE,
   
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
   coalesce(A.day_diff, B.day_diff) as day_diff,
   coalesce(A.sec_diff, B.sec_diff) as sec_diff,
   coalesce(A.is_same_device, B.is_same_device) as is_same_device,
   coalesce(A.is_same_exp, B.is_same_exp) as is_same_exp,
   coalesce(A.data_source, B.data_source) as data_source



   FROM 			uid_join         A
   FULL OUTER JOIN		deviceid_join      B
   ON   	   		A.event_id       = B.event_id
   AND			        A.item_id        = B.item_id
   AND			        A.transaction_id = B.transaction_id   
   """

   val uid_deviceid_join_df = sql(uid_deviceid_join_query)
   uid_deviceid_join_df.createOrReplaceTempView("uid_deviceid_join")







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
   coalesce(A.FM_BUYER_TYPE_CD, B.FM_BUYER_TYPE_CD) as FM_BUYER_TYPE_CD,
   coalesce(A.FM_BUYER_TYPE_DESC, B.FM_BUYER_TYPE_DESC) as FM_BUYER_TYPE_DESC,
   coalesce(A.IS_FIRST_PURCHASE, B.IS_FIRST_PURCHASE) as IS_FIRST_PURCHASE,
   
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
   coalesce(A.day_diff, B.day_diff) as day_diff,
   coalesce(A.sec_diff, B.sec_diff) as sec_diff,
   coalesce(A.is_same_device, B.is_same_device) as is_same_device,
   coalesce(A.is_same_exp, B.is_same_exp) as is_same_exp,
   coalesce(A.data_source, B.data_source) as data_source



   FROM 			cguid_join         A
   FULL OUTER JOIN		xid_join           B
   ON   	   		A.event_id       = B.event_id
   AND			        A.item_id        = B.item_id
   AND			        A.transaction_id = B.transaction_id   
   """

   val cguid_xid_join_df = sql(cguid_xid_join_query)
   cguid_xid_join_df.createOrReplaceTempView("cguid_xid_join")
   




   val merge_all_join_query = """
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
   coalesce(A.FM_BUYER_TYPE_CD, B.FM_BUYER_TYPE_CD) as FM_BUYER_TYPE_CD,
   coalesce(A.FM_BUYER_TYPE_DESC, B.FM_BUYER_TYPE_DESC) as FM_BUYER_TYPE_DESC,
   coalesce(A.IS_FIRST_PURCHASE, B.IS_FIRST_PURCHASE) as IS_FIRST_PURCHASE,
   
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
   coalesce(A.day_diff, B.day_diff) as day_diff,
   coalesce(A.sec_diff, B.sec_diff) as sec_diff,
   coalesce(A.is_same_device, B.is_same_device) as is_same_device,
   coalesce(A.is_same_exp, B.is_same_exp) as is_same_exp,
   coalesce(A.data_source, B.data_source) as data_source,
   coalesce(A.created_dt, B.created_dt)   as cko_dt,
   coalesce(A.event_dt, B.event_dt)       as mktng_dt


   FROM 			uid_deviceid_join         A
   FULL OUTER JOIN		cguid_xid_join            B
   ON   	   		A.event_id       = B.event_id
   AND			        A.item_id        = B.item_id
   AND			        A.transaction_id = B.transaction_id   
   """

   val merge_all_join_df = sql(merge_all_join_query)
   merge_all_join_df
   .repartition($"data_source",$"cko_dt",$"mktng_dt")
   .write
   .partitionBy("data_source","cko_dt","mktng_dt")
   .save("hdfs://apollo-phx-nn-ha/user/hive/warehouse/mktng.db/me_txns_correlation")
   
   







  }
}
