import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.Column


object Sess_UNSIGNED_CkoBbowaVI_MktngCorrelationApp {

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



   val sess_start_dt   = args(0)
   val sess_end_dt     = args(1)
   val mktng_start_dt = args(2)
   val mktng_end_dt   = sess_end_dt

   // ========================


   // ========================
   // M EVENTS
   // ========================
   //val mktng_path   = s"hdfs://apollo-phx-nn-ha/user/hive/warehouse/mktng.db/marketing_events_parquet"

   val mktng_path   = s"hdfs://apollo-phx-nn-ha/user/hive/warehouse/mktng.db/marketing_clicks_parquet_bigfiles"

   // ========================
   // SESSIONS
   // ========================
   val sess_path = "hdfs://apollo-phx-nn-ha/user/hive/warehouse/mktng.db/sess_cko_bbowa_vi_with_xid"
   // ========================


   // ========================
   //LOAD FRAMES
   // ========================
   val mevents_df    = spark.read.load(mktng_path)
   val sess_xid_df   = spark.read.load(sess_path)
   // ========================




   // ========================
   //FILTER FOR DATES
   // ========================
   //ONLY UNSIGNED!
   val sess_df   = sess_xid_df.filter(s"dt between '$sess_start_dt' and '$sess_end_dt' and incdata_id is null and coalesce(parent_uid,0)=0").sample(false, 0.2)
   sess_df.cache()

   //TAKE ONLY CLICKS
   val mktng_df = mevents_df.filter(s"dt between '$mktng_start_dt' and '$mktng_end_dt' and event_type_id in (1,5,8)")

   sess_df.createOrReplaceTempView("sess_xid")
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
   B.guid as sess_guid,
   B.cguid as sess_cguid,
   B.parent_uid as sess_parent_uid,
   B.session_skey as sess_session_skey,
   B.site_id as sess_site_id,
   B.cobrand as sess_cobrand,
   B.session_start_dt as sess_session_start_dt,
   B.start_timestamp as sess_start_timestamp,
   B.end_timestamp as sess_end_timestamp,
   B.primary_app_id as sess_primary_app_id,
   B.session_traffic_source_id as sess_session_traffic_source_id,
   B.cust_traffic_source_level1 as sess_cust_traffic_source_level1,
   B.cust_traffic_source_level2 as sess_cust_traffic_source_level2,
   B.traffic_source_level3 as sess_traffic_source_level3,
   B.rotation_id as sess_rotation_id,
   B.rvr_id as sess_rvr_id,
   B.idfa as sess_idfa,
   B.gadid as sess_gadid,
   B.gdid as sess_gdid,
   B.device_id as sess_device_id,
   B.device_type as sess_device_type,
   B.device_type_level1 as sess_device_type_level1,
   B.device_type_level2 as sess_device_type_level2,
   B.experience_level1 as sess_experience_level1,
   B.experience_level2 as sess_experience_level2,
   B.vi_cnt as sess_vi_cnt,
   B.num_txns as sess_num_txns,
   B.gmb_usd as sess_gmb_usd,
   B.has_first_purchase as sess_has_first_purchase,
   B.num_watch as sess_num_watch,
   B.num_ask as sess_num_ask,
   B.num_ac as sess_num_ac,
   B.num_bo as sess_num_bo,
   B.num_bb as sess_num_bb,
   B.num_bbowa as sess_num_bbowa,
   B.num_meta as sess_num_meta,
   B.num_lv2 as sess_num_lv2,
   B.num_lv3 as sess_num_lv3,
   B.num_lv4 as sess_num_lv4,
   B.specificity as sess_specificity,
   B.specificity_binned as sess_specificity_binned,
   B.n_registrations   as sess_n_registrations,
   B.num_fashion as sess_num_fashion,
   B.num_home_garden as sess_num_home_garden,
   B.num_electronics as sess_num_electronics,
   B.num_collectibles as sess_num_collectibles,
   B.num_parts_acc as sess_num_parts_acc,
   B.num_vehicles as sess_num_vehicles,
   B.num_lifestyle as sess_num_lifestyle,
   B.num_bu_industrial as sess_num_bu_industrial,
   B.num_media as sess_num_media,
   B.num_real_estate as sess_num_real_estate,
   B.num_unknown     as sess_num_unknown,     
   B.num_clothes_shoes_acc as sess_num_clothes_shoes_acc,
   B.num_home_furniture as sess_num_home_furniture,
   B.num_cars_motors_vehic as sess_num_cars_motors_vehic,
   B.num_cars_p_and_acc as sess_num_cars_p_and_acc,
   B.num_sporting_goods as sess_num_sporting_goods,
   B.num_toys_games as sess_num_toys_games,
   B.num_health_beauty as sess_num_health_beauty,
   B.num_mob_phones as sess_num_mob_phones,
   B.num_business_ind as sess_num_business_ind,
   B.num_baby as sess_num_baby,
   B.num_jewellry_watches as sess_num_jewellry_watches,
   B.num_computers as sess_num_computers,
   B.num_modellbau as sess_num_modellbau,
   B.num_antiquities as sess_num_antiquities,
   B.num_sound_vision as sess_num_sound_vision,
   B.num_video_games_consoles as sess_num_video_games_consoles,
   B.join_strategy as sess_join_strategy,
   B.incdata_id as sess_incdata_id,
   (unix_timestamp(B.start_timestamp) - unix_timestamp(A.event_ts))/86400. as day_diff,
   (unix_timestamp(B.start_timestamp) - unix_timestamp(A.event_ts))        as sec_diff,   
   """


   sql("select * from mktng    where user_id >0").createOrReplaceTempView("mktng_non_null_uid")
   sql("select * from sess_xid where parent_uid >0").createOrReplaceTempView("sess_xid_non_null_uid")
   val uid_query_tail = """
   "uid"                 as mktng_join_strategy

   FROM        mktng_non_null_uid       A
   INNER JOIN  sess_xid_non_null_uid    B
   on          A.user_id = B.parent_uid

   where (A.event_dt   <= B.session_start_dt)
   and  (datediff(B.session_start_dt, A.event_dt) <= 14)
   and  (unix_timestamp(A.event_ts) <= unix_timestamp(B.start_timestamp) + 600)
   and  (unix_timestamp(B.start_timestamp) -  unix_timestamp(A.event_ts) <= 14*86400)   
   """


   val cguid_query_tail = """
   "cguid"                 as mktng_join_strategy

   FROM        mktng      A
   INNER JOIN  sess_xid    B
   on          A.CGUID = B.CGUID

   where (A.event_dt   <= B.session_start_dt)
   and  (datediff(B.session_start_dt, A.event_dt) <= 14)
   and  (unix_timestamp(A.event_ts) <= unix_timestamp(B.start_timestamp) + 600)
   and  (unix_timestamp(B.start_timestamp) -  unix_timestamp(A.event_ts) <= 14*86400)
   """




   sql("""select * from mktng    where LENGTH(device_id)>=35 and device_id not like '%00000000-0000-0000-0000-000000000000%' """).createOrReplaceTempView("mktng_non_null_did")
   sql("""select * from sess_xid where LENGTH(device_id)>=35 and device_id not like '%00000000-0000-0000-0000-000000000000%' """).createOrReplaceTempView("sess_xid_non_null_did")   
   val did_query_tail = """
   "did"                 as mktng_join_strategy

   FROM        mktng_non_null_did       A
   INNER JOIN  sess_xid_non_null_did    B
   on          A.device_id = B.device_id

   where (A.event_dt   <= B.session_start_dt)
   and  (datediff(B.session_start_dt, A.event_dt) <= 14)
   and  (unix_timestamp(A.event_ts) <= unix_timestamp(B.start_timestamp) + 600)
   and  (unix_timestamp(B.start_timestamp) -  unix_timestamp(A.event_ts) <= 14*86400)   
   """


   sql("select * from mktng where    incdata_id is not null").createOrReplaceTempView("mktng_non_null_xid")
   sql("select * from sess_xid where incdata_id is not null").createOrReplaceTempView("sess_xid_non_null_xid")

   val xid_query_tail = """
   "xid"                 as mktng_join_strategy
   
   FROM        mktng_non_null_xid      A
   INNER JOIN  sess_xid_non_null_xid    B
   on          A.incdata_id = B.incdata_id

   where (A.event_dt   <= B.session_start_dt)
   and  (datediff(B.session_start_dt, A.event_dt) <= 14)
   and  (unix_timestamp(A.event_ts) <= unix_timestamp(B.start_timestamp) + 600)
   and  (unix_timestamp(B.start_timestamp) -  unix_timestamp(A.event_ts) <= 14*86400)   
   """



   sql(common_join_sql+  uid_query_tail).createOrReplaceTempView("uid_join")
   sql(common_join_sql+cguid_query_tail).createOrReplaceTempView("cguid_join")
   sql(common_join_sql+  did_query_tail).createOrReplaceTempView("did_join")
   sql(common_join_sql  +xid_query_tail).createOrReplaceTempView("xid_join")   


   //
   //
   //
   //
   //
   val full_join_sql = """
   select
   coalesce(A.guid,B.guid,C.guid,D.guid) as guid,
   coalesce(A.cguid,B.cguid,C.cguid,D.cguid) as cguid,
   coalesce(A.user_id,B.user_id,C.user_id,D.user_id) as user_id,
   coalesce(A.event_id,B.event_id,C.event_id,D.event_id) as event_id,
   coalesce(A.event_id_type,B.event_id_type,C.event_id_type,D.event_id_type) as event_id_type,
   coalesce(A.event_type_id,B.event_type_id,C.event_type_id,D.event_type_id) as event_type_id,
   coalesce(A.channel_id,B.channel_id,C.channel_id,D.channel_id) as channel_id,
   coalesce(A.channel_name,B.channel_name,C.channel_name,D.channel_name) as channel_name,
   coalesce(A.rotation_id,B.rotation_id,C.rotation_id,D.rotation_id) as rotation_id,
   coalesce(A.rotation_name,B.rotation_name,C.rotation_name,D.rotation_name) as rotation_name,
   coalesce(A.campaign_id,B.campaign_id,C.campaign_id,D.campaign_id) as campaign_id,
   coalesce(A.campaign_name,B.campaign_name,C.campaign_name,D.campaign_name) as campaign_name,
   coalesce(A.event_dt,B.event_dt,C.event_dt,D.event_dt) as event_dt,
   coalesce(A.event_ts,B.event_ts,C.event_ts,D.event_ts) as event_ts,
   coalesce(A.incdata_id,B.incdata_id,C.incdata_id,D.incdata_id) as incdata_id,
   coalesce(A.cluster_id,B.cluster_id,C.cluster_id,D.cluster_id) as cluster_id,
   coalesce(A.device_id,B.device_id,C.device_id,D.device_id) as device_id,
   coalesce(A.experience_level1,B.experience_level1,C.experience_level1,D.experience_level1) as experience_level1,
   coalesce(A.experience_level2,B.experience_level2,C.experience_level2,D.experience_level2) as experience_level2,
   coalesce(A.device_type_level1,B.device_type_level1,C.device_type_level1,D.device_type_level1) as device_type_level1,
   coalesce(A.device_type_level2,B.device_type_level2,C.device_type_level2,D.device_type_level2) as device_type_level2,
   coalesce(A.flex_column_1,B.flex_column_1,C.flex_column_1,D.flex_column_1) as flex_column_1,
   coalesce(A.flex_column_2,B.flex_column_2,C.flex_column_2,D.flex_column_2) as flex_column_2,
   coalesce(A.flex_column_3,B.flex_column_3,C.flex_column_3,D.flex_column_3) as flex_column_3,
   coalesce(A.flex_column_4,B.flex_column_4,C.flex_column_4,D.flex_column_4) as flex_column_4,
   coalesce(A.flex_column_5,B.flex_column_5,C.flex_column_5,D.flex_column_5) as flex_column_5,
   coalesce(A.flex_column_6,B.flex_column_6,C.flex_column_6,D.flex_column_6) as flex_column_6,
   
   coalesce(A.sess_guid,B.sess_guid,C.sess_guid,D.sess_guid) as sess_guid,
   coalesce(A.sess_cguid,B.sess_cguid,C.sess_cguid,D.sess_cguid) as sess_cguid,
   coalesce(A.sess_parent_uid,B.sess_parent_uid,C.sess_parent_uid,D.sess_parent_uid) as sess_parent_uid,
   coalesce(A.sess_session_skey,B.sess_session_skey,C.sess_session_skey,D.sess_session_skey) as sess_session_skey,
   coalesce(A.sess_site_id,B.sess_site_id,C.sess_site_id,D.sess_site_id) as sess_site_id,
   coalesce(A.sess_cobrand,B.sess_cobrand,C.sess_cobrand,D.sess_cobrand) as sess_cobrand,
   coalesce(A.sess_session_start_dt,B.sess_session_start_dt,C.sess_session_start_dt,D.sess_session_start_dt) as sess_session_start_dt,
   coalesce(A.sess_start_timestamp,B.sess_start_timestamp,C.sess_start_timestamp,D.sess_start_timestamp) as sess_start_timestamp,
   coalesce(A.sess_end_timestamp,B.sess_end_timestamp,C.sess_end_timestamp,D.sess_end_timestamp) as sess_end_timestamp,
   coalesce(A.sess_primary_app_id,B.sess_primary_app_id,C.sess_primary_app_id,D.sess_primary_app_id) as sess_primary_app_id,
   coalesce(A.sess_session_traffic_source_id,B.sess_session_traffic_source_id,C.sess_session_traffic_source_id,D.sess_session_traffic_source_id) as sess_session_traffic_source_id,
   coalesce(A.sess_cust_traffic_source_level1,B.sess_cust_traffic_source_level1,C.sess_cust_traffic_source_level1,D.sess_cust_traffic_source_level1) as sess_cust_traffic_source_level1,
   coalesce(A.sess_cust_traffic_source_level2,B.sess_cust_traffic_source_level2,C.sess_cust_traffic_source_level2,D.sess_cust_traffic_source_level2) as sess_cust_traffic_source_level2,
   coalesce(A.sess_traffic_source_level3,B.sess_traffic_source_level3,C.sess_traffic_source_level3,D.sess_traffic_source_level3) as sess_traffic_source_level3,
   coalesce(A.sess_rotation_id,B.sess_rotation_id,C.sess_rotation_id,D.sess_rotation_id) as sess_rotation_id,
   coalesce(A.sess_rvr_id,B.sess_rvr_id,C.sess_rvr_id,D.sess_rvr_id) as sess_rvr_id,
   coalesce(A.sess_idfa,B.sess_idfa,C.sess_idfa,D.sess_idfa) as sess_idfa,
   coalesce(A.sess_gadid,B.sess_gadid,C.sess_gadid,D.sess_gadid) as sess_gadid,
   coalesce(A.sess_gdid,B.sess_gdid,C.sess_gdid,D.sess_gdid) as sess_gdid,
   coalesce(A.sess_device_id,B.sess_device_id,C.sess_device_id,D.sess_device_id) as sess_device_id,
   coalesce(A.sess_device_type,B.sess_device_type,C.sess_device_type,D.sess_device_type) as sess_device_type,
   coalesce(A.sess_device_type_level1,B.sess_device_type_level1,C.sess_device_type_level1,D.sess_device_type_level1) as sess_device_type_level1,
   coalesce(A.sess_device_type_level2,B.sess_device_type_level2,C.sess_device_type_level2,D.sess_device_type_level2) as sess_device_type_level2,
   coalesce(A.sess_experience_level1,B.sess_experience_level1,C.sess_experience_level1,D.sess_experience_level1) as sess_experience_level1,
   coalesce(A.sess_experience_level2,B.sess_experience_level2,C.sess_experience_level2,D.sess_experience_level2) as sess_experience_level2,
   coalesce(A.sess_vi_cnt,B.sess_vi_cnt,C.sess_vi_cnt,D.sess_vi_cnt)                     as sess_vi_cnt,
   coalesce(A.sess_num_txns,B.sess_num_txns,C.sess_num_txns,D.sess_num_txns)             as sess_num_txns,
   coalesce(A.sess_gmb_usd,B.sess_gmb_usd,C.sess_gmb_usd,D.sess_gmb_usd)                 as sess_gmb_usd,
   coalesce(A.sess_has_first_purchase,B.sess_has_first_purchase,C.sess_has_first_purchase,D.sess_has_first_purchase) as sess_has_first_purchase,
   coalesce(A.sess_num_watch,B.sess_num_watch,C.sess_num_watch,D.sess_num_watch)         as sess_num_watch,
   coalesce(A.sess_num_ask,B.sess_num_ask,C.sess_num_ask,D.sess_num_ask)                 as sess_num_ask,
   coalesce(A.sess_num_ac,B.sess_num_ac,C.sess_num_ac,D.sess_num_ac)                     as sess_num_ac,
   coalesce(A.sess_num_bo,B.sess_num_bo,C.sess_num_bo,D.sess_num_bo)                     as sess_num_bo,
   coalesce(A.sess_num_bb,B.sess_num_bb,C.sess_num_bb,D.sess_num_bb)                     as sess_num_bb,
   coalesce(A.sess_num_bbowa,B.sess_num_bbowa,C.sess_num_bbowa,D.sess_num_bbowa)         as sess_num_bbowa,
   coalesce(A.sess_num_meta,B.sess_num_meta,C.sess_num_meta,D.sess_num_meta)             as sess_num_meta,
   coalesce(A.sess_num_lv2,B.sess_num_lv2,C.sess_num_lv2,D.sess_num_lv2)                 as sess_num_lv2,
   coalesce(A.sess_num_lv3,B.sess_num_lv3,C.sess_num_lv3,D.sess_num_lv3)                 as sess_num_lv3,
   coalesce(A.sess_num_lv4,B.sess_num_lv4,C.sess_num_lv4,D.sess_num_lv4)                 as sess_num_lv4,
   coalesce(A.sess_specificity,B.sess_specificity,C.sess_specificity,D.sess_specificity) as sess_specificity,
   coalesce(A.sess_specificity_binned,B.sess_specificity_binned,C.sess_specificity_binned,D.sess_specificity_binned) as sess_specificity_binned,
   coalesce(A.sess_n_registrations,B.sess_n_registrations,C.sess_n_registrations,D.sess_n_registrations)             as sess_n_registrations,
   coalesce(A.sess_num_fashion,B.sess_num_fashion,C.sess_num_fashion,D.sess_num_fashion) as sess_num_fashion,
   coalesce(A.sess_num_home_garden,B.sess_num_home_garden,C.sess_num_home_garden,D.sess_num_home_garden) as sess_num_home_garden,
   coalesce(A.sess_num_electronics,B.sess_num_electronics,C.sess_num_electronics,D.sess_num_electronics) as sess_num_electronics,
   coalesce(A.sess_num_collectibles,B.sess_num_collectibles,C.sess_num_collectibles,D.sess_num_collectibles) as sess_num_collectibles,
   coalesce(A.sess_num_parts_acc,B.sess_num_parts_acc,C.sess_num_parts_acc,D.sess_num_parts_acc) as sess_num_parts_acc,
   coalesce(A.sess_num_vehicles,B.sess_num_vehicles,C.sess_num_vehicles,D.sess_num_vehicles) as sess_num_vehicles,
   coalesce(A.sess_num_lifestyle,B.sess_num_lifestyle,C.sess_num_lifestyle,D.sess_num_lifestyle) as sess_num_lifestyle,
   coalesce(A.sess_num_bu_industrial,B.sess_num_bu_industrial,C.sess_num_bu_industrial,D.sess_num_bu_industrial) as sess_num_bu_industrial,
   coalesce(A.sess_num_media,B.sess_num_media,C.sess_num_media,D.sess_num_media) as sess_num_media,
   coalesce(A.sess_num_real_estate,B.sess_num_real_estate,C.sess_num_real_estate,D.sess_num_real_estate) as sess_num_real_estate,
   coalesce(A.sess_num_unknown,     B.sess_num_unknown,     C.sess_num_unknown,     D.sess_num_unknown    ) as sess_num_unknown,     
   coalesce(A.sess_num_clothes_shoes_acc,B.sess_num_clothes_shoes_acc,C.sess_num_clothes_shoes_acc,D.sess_num_clothes_shoes_acc) as sess_num_clothes_shoes_acc,
   coalesce(A.sess_num_home_furniture,B.sess_num_home_furniture,C.sess_num_home_furniture,D.sess_num_home_furniture) as sess_num_home_furniture,
   coalesce(A.sess_num_cars_motors_vehic,B.sess_num_cars_motors_vehic,C.sess_num_cars_motors_vehic,D.sess_num_cars_motors_vehic) as sess_num_cars_motors_vehic,
   coalesce(A.sess_num_cars_p_and_acc,B.sess_num_cars_p_and_acc,C.sess_num_cars_p_and_acc,D.sess_num_cars_p_and_acc) as sess_num_cars_p_and_acc,
   coalesce(A.sess_num_sporting_goods,B.sess_num_sporting_goods,C.sess_num_sporting_goods,D.sess_num_sporting_goods) as sess_num_sporting_goods,
   coalesce(A.sess_num_toys_games,B.sess_num_toys_games,C.sess_num_toys_games,D.sess_num_toys_games) as sess_num_toys_games,
   coalesce(A.sess_num_health_beauty,B.sess_num_health_beauty,C.sess_num_health_beauty,D.sess_num_health_beauty) as sess_num_health_beauty,
   coalesce(A.sess_num_mob_phones,B.sess_num_mob_phones,C.sess_num_mob_phones,D.sess_num_mob_phones) as sess_num_mob_phones,
   coalesce(A.sess_num_business_ind,B.sess_num_business_ind,C.sess_num_business_ind,D.sess_num_business_ind) as sess_num_business_ind,
   coalesce(A.sess_num_baby,B.sess_num_baby,C.sess_num_baby,D.sess_num_baby) as sess_num_baby,
   coalesce(A.sess_num_jewellry_watches,B.sess_num_jewellry_watches,C.sess_num_jewellry_watches,D.sess_num_jewellry_watches) as sess_num_jewellry_watches,
   coalesce(A.sess_num_computers,B.sess_num_computers,C.sess_num_computers,D.sess_num_computers) as sess_num_computers,
   coalesce(A.sess_num_modellbau,B.sess_num_modellbau,C.sess_num_modellbau,D.sess_num_modellbau) as sess_num_modellbau,
   coalesce(A.sess_num_antiquities,B.sess_num_antiquities,C.sess_num_antiquities,D.sess_num_antiquities) as sess_num_antiquities,
   coalesce(A.sess_num_sound_vision,B.sess_num_sound_vision,C.sess_num_sound_vision,D.sess_num_sound_vision) as sess_num_sound_vision,
   coalesce(A.sess_num_video_games_consoles,B.sess_num_video_games_consoles, C.sess_num_video_games_consoles, D.sess_num_video_games_consoles) as sess_num_video_games_consoles,
   coalesce(A.sess_join_strategy,B.sess_join_strategy,C.sess_join_strategy,D.sess_join_strategy)                     as sess_join_strategy,
   coalesce(A.sess_incdata_id,B.sess_incdata_id,C.sess_incdata_id,D.sess_incdata_id)                                 as sess_incdata_id,

   coalesce(A.day_diff,B.day_diff,C.day_diff,D.day_diff) as day_diff,
   coalesce(A.sec_diff,B.sec_diff,C.sec_diff,D.sec_diff) as sec_diff,
   coalesce(A.mktng_join_strategy,B.mktng_join_strategy,C.mktng_join_strategy,D.mktng_join_strategy) as mktng_join_strategy,
   coalesce(A.sess_session_start_dt, B.sess_session_start_dt, C.sess_session_start_dt, D.sess_session_start_dt) as sess_dt,
   coalesce(A.event_dt, B.event_dt, C.event_dt, D.event_dt) as mktng_dt
   FROM             uid_join                A
   FULL OUTER JOIN  did_join                B
   ON               A.event_id           =  B.event_id
   AND              A.sess_guid          =  B.sess_guid
   AND              A.sess_session_skey  =  B.sess_session_skey
   FULL OUTER JOIN  cguid_join              C
   ON               A.event_id           =  C.event_id
   AND              A.sess_guid          =  C.sess_guid
   AND              A.sess_session_skey  =  C.sess_session_skey
   FULL OUTER JOIN  xid_join                D
   ON               A.event_id           =  D.event_id
   AND              A.sess_guid          =  D.sess_guid
   AND              A.sess_session_skey  =  D.sess_session_skey

   """ 
   

   val rNumb = (row_number()
		.over(Window.partitionBy(col("sess_guid"), col("sess_session_skey"), col("event_id"))
		      .orderBy(col("device_type_level1"), col("device_type_level2"),col("experience_level1"),col("experience_level2"))
		    )
              )

   val full_join_df = (spark
		       .sql(full_join_sql)
		       .withColumn("rnum", rNumb)
		       .filter($"rnum"===1)
		       )
     
   (full_join_df
   .write
   .partitionBy("sess_dt", "mktng_dt")
   .mode(SaveMode.Append)
   .save("hdfs://apollo-phx-nn-ha/user/hive/warehouse/mktng.db/sess_unsigned_cko_bbowa_vi_mktng_corr")
  )
 }
}
