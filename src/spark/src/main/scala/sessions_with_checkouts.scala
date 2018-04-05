import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object Sess_CKO_BBOWA_VI_APP {

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


   val sess_start_dt    = args(0)
   val sess_end_dt     = args(1)



   // ========================   
   //SESSIONS
   // ========================
   val sessions_df = spark.read.load("hdfs://apollo-phx-nn-ha/user/hive/warehouse/mktng.db/clav_sess_parquet")
   sessions_df.createOrReplaceTempView("sessions_all")
   spark.sql(s"select * from sessions_all where dt between '${sess_start_dt}' and '${sess_end_dt}'").createOrReplaceTempView("sessions")

   // ========================
   //CHECKOUTS
   // ========================
   val txns_df     = spark.read.load("hdfs://apollo-phx-nn-ha/user/hive/warehouse/txns.db/txn_events_with_bbowa_incdata")
   txns_df.createOrReplaceTempView("transactions")


   // ========================
   //BBOWA EVENTS
   // ========================
   val bbowa_path  = "hdfs://apollo-phx-nn-ha/user/hive/warehouse/txns.db/bbowa_parquet"
   spark.read.load(bbowa_path).createOrReplaceTempView("bbowa_all")
   // ========================



   // ========================
   // SPECIFICITY
   // ========================
   val spec_path  = "hdfs://apollo-phx-nn-ha/user/hive/warehouse/mktng.db/specificity_parquet_ana"
   spark.read.load(spec_path).createOrReplaceTempView("specificity_all")
   spark.sql(s"select * from specificity_all where dt between '${sess_start_dt}' and '${sess_end_dt}'")createOrReplaceTempView("specificity")
   // ========================


   //=========================
   //VIEW ITEMS 
   //=========================
   val items_path = s"hdfs://apollo-phx-nn-ha/user/hive/warehouse/mktng.db/view_items_cats_parquet_external_ana"

   spark.read.load(items_path).createOrReplaceTempView("items_all")
   val items_sql = s"""
   select
   guid,
   session_skey,
   site_id,
   sum( case when bsns_vrtcl_name = 'Fashion' then 1 else 0 end )               as num_fashion,
   sum( case when bsns_vrtcl_name = 'Home & Garden' then 1 else 0 end )         as num_home_garden,
   sum( case when bsns_vrtcl_name = 'Electronics' then 1 else 0 end )           as num_electronics,
   sum( case when bsns_vrtcl_name = 'Collectibles' then 1 else 0 end )          as num_collectibles,
   sum( case when bsns_vrtcl_name = 'Parts & Accessories' then 1 else 0 end )   as num_parts_acc,
   sum( case when bsns_vrtcl_name = 'Vehicles' then 1 else 0 end )              as num_vehicles,
   sum( case when bsns_vrtcl_name = 'Lifestyle' then 1 else 0 end )             as num_lifestyle,
   sum( case when bsns_vrtcl_name = 'Business & Industrial' then 1 else 0 end ) as num_bu_industrial,
   sum( case when bsns_vrtcl_name = 'Media' then 1 else 0 end )                 as num_media,
   sum( case when bsns_vrtcl_name = 'Real Estate' then 1 else 0 end )           as num_real_estate,
   sum( case when bsns_vrtcl_name = 'Unknown' then 1 else 0 end )               as num_unknown,     
   sum( case when meta_categ_id   = 11450 then 1 else 0 end )                   as num_clothes_shoes_acc,
   sum( case when meta_categ_id   = 11700 then 1 else 0 end )                   as num_home_furniture,
   sum( case when meta_categ_id   = 9800 then 1 else 0 end )                    as num_cars_motors_vehic,
   sum( case when meta_categ_id   = 131090 then 1 else 0 end )                  as num_cars_p_and_acc,
   sum( case when meta_categ_id   = 888 then 1 else 0 end )                     as num_sporting_goods,
   sum( case when meta_categ_id   = 220 then 1 else 0 end )                     as num_toys_games,
   sum( case when meta_categ_id   = 26395 then 1 else 0 end )                   as num_health_beauty,
   sum( case when meta_categ_id   = 15032 then 1 else 0 end )                   as num_mob_phones,
   sum( case when meta_categ_id   = 12576 then 1 else 0 end )                   as num_business_ind,
   sum( case when meta_categ_id   = 2984 then 1 else 0 end )                    as num_baby,
   sum( case when meta_categ_id   = 281  then 1 else 0 end )                    as num_jewellry_watches,
   sum( case when meta_categ_id   = 58058  then 1 else 0 end )                  as num_computers,
   sum( case when meta_categ_id   = 22128  then 1 else 0 end )                  as num_modellbau,
   sum( case when meta_categ_id   = 353    then 1 else 0 end )                  as num_antiquities,
   sum( case when meta_categ_id   = 293    then 1 else 0 end )                  as num_sound_vision,
   sum( case when meta_categ_id   = 1249    then 1 else 0 end )                 as num_video_games_consoles
   
   from items_all
   where dt between '${sess_start_dt}' and '${sess_end_dt}'
   group by 
   guid,
   session_skey,
   site_id
   """
   spark.sql(items_sql).createOrReplaceTempView("items")
   



   // ========================
   // REGISTRATIONS
   // ========================
   val reg_path  = "hdfs://apollo-phx-nn-ha/user/hive/warehouse/mktng.db/user_regristration_parquet"
   spark.read.load(reg_path).createOrReplaceTempView("registrations_all")
   val reg_sql = s"""   
   select 
   guid,
   session_skey,
   site_id,
   cobrand,
   count(*) as n_registrations
   from registrations_all
   where dt between '${sess_start_dt}' and '${sess_end_dt}'
   group by
   guid,
   session_skey,
   site_id,
   cobrand
   """
   spark.sql(reg_sql).createOrReplaceTempView("registrations")
   // ========================









	    
   // ========================
   val group_sql = s"""
   select
   sess_guid,
   sess_session_skey,
   sess_cguid,
   sess_site_id,
   sess_cobrand,
   count(*) as num_txns,
   sum(gmb_usd) as gmb_usd,
   sum(IS_FIRST_PURCHASE) as has_first_purchase

   from  transactions
   where dt between '${sess_start_dt}' and DATE_ADD('${sess_end_dt}', 2)
   group by
   sess_guid,
   sess_session_skey,
   sess_cguid,
   sess_site_id,
   sess_cobrand
   """

   val gdf = spark.sql(group_sql)
   gdf.createOrReplaceTempView("cko_on_sess")



   //===========================
   //BBOWA GROUPBY SESSION
   //===========================

   val bbowa_sql  = s"""
   select
   guid,
   session_skey,
   site_id,
   cobrand,
   sum(case when event_type = "Watch" then 1 else 0 end ) as num_watch,
   sum(case when event_type = "Ask"   then 1 else 0 end ) as num_ask,
   sum(case when event_type = "AC"    then 1 else 0 end ) as num_ac,
   sum(case when event_type = "BO"    then 1 else 0 end ) as num_bo,
   sum(case when event_type = "BB"    then 1 else 0 end ) as num_bb,
   count(*) as num_bbowa
   from bbowa_all
   where dt between '${sess_start_dt}' and DATE_ADD('${sess_end_dt}', 1)
   group by
   guid,
   session_skey,
   site_id,
   cobrand
   """
   spark.sql(bbowa_sql).createOrReplaceTempView("bbowa_grouped")




   //================================================================
   //
   //FULL JOIN
   //
   //================================================================
   val sess_cko_bbowa_vi_sql = s"""
   select
   A.*,
   B.num_txns,
   B.gmb_usd,
   B.has_first_purchase,
   C.num_watch,
   C.num_ask,
   C.num_ac,
   C.num_bo,
   C.num_bb,
   C.num_bbowa,
   D.num_meta,
   D.num_lv2,
   D.num_lv3,
   D.num_lv4,
   D.specificity,
   D.specificity_binned,
   E.n_registrations,
   F.num_fashion,
   F.num_home_garden,
   F.num_electronics,
   F.num_collectibles,
   F.num_parts_acc,
   F.num_vehicles,
   F.num_lifestyle,
   F.num_bu_industrial,
   F.num_media,
   F.num_real_estate,
   F.num_unknown,     
   F.num_clothes_shoes_acc,
   F.num_home_furniture,
   F.num_cars_motors_vehic,
   F.num_cars_p_and_acc,
   F.num_sporting_goods,
   F.num_toys_games,
   F.num_health_beauty,
   F.num_mob_phones,
   F.num_business_ind,
   F.num_baby,
   F.num_jewellry_watches,
   F.num_computers,
   F.num_modellbau,
   F.num_antiquities,
   F.num_sound_vision,
   F.num_video_games_consoles

   from        sessions A
   left join   cko_on_sess B

   on          A.guid         = B.sess_guid
   and         A.session_skey = B.sess_session_skey
   and         A.site_id      = B.sess_site_id
   and         A.cobrand      = B.sess_cobrand

   left join   bbowa_grouped    C
   on          A.guid         = C.guid
   and         A.session_skey = C.session_skey
   and         A.site_id      = C.site_id
   and         A.cobrand      = C.cobrand

   left join   specificity      D
   on          A.guid         = D.guid
   and         A.session_skey = D.session_skey
   and         A.site_id      = D.site_id

xo   left join   registrations    E
   on          A.guid         = E.guid
   and         A.session_skey = E.session_skey
   and         A.site_id      = E.site_id   

   left join   items            F
   on          A.guid         = F.guid
   and         A.session_skey = F.session_skey
   and         A.site_id      = F.site_id
   """ 
   //
   //
   //
   //QUALIFIER
   val rNumb = (row_number()
		.over(Window.partitionBy(col("guid"), col("session_skey"), col("site_id"), col("cobrand"))
		      .orderBy(col("device_type_level1"), col("device_type_level2"),col("experience_level1"),col("experience_level2"))
			)
              )

   val sess_cko_bbowa_vi_df = (spark
			       .sql(sess_cko_bbowa_vi_sql)
			       .select($"*", rNumb as "rnum")
			       .filter($"rnum"===1)
			       )

   //==========================================================================
   (sess_cko_bbowa_vi_df
   .write
   .partitionBy("dt")
   .mode(SaveMode.Append)
   .save("hdfs://apollo-phx-nn-ha/user/hive/warehouse/mktng.db/sess_cko_bbowa_vi")
  )
   //==========================================================================




   //==========================================================================
   // JOIN TO XID
   //==========================================================================
   //
   val sess_cko_bbowa_vi_read_df = spark.read.load("hdfs://apollo-phx-nn-ha/user/hive/warehouse/mktng.db/sess_cko_bbowa_vi")
   sess_cko_bbowa_vi_read_df.createOrReplaceTempView("sess_cko_bbowa_vi_all")
   spark.sql(s"select * from sess_cko_bbowa_vi_all where dt between '${sess_start_dt}' and '${sess_end_dt}' ").createOrReplaceTempView("sess_cko_bbowa_vi")
   //
   //
   //
   //===========================
   // XID 
   //===========================
   val inc_master_df   = spark.read.load("hdfs://apollo-phx-nn-ha/user/hive/warehouse/mktng.db/inc_master_low_skew_parquet")
   val cguid_to_xid_df = spark.read.load("hdfs://apollo-phx-nn-ha/user/hive/warehouse/mktng.db/incdata_cguid_to_xid_low_skew_parquet")
   val did_to_xid_df   = spark.read.load("hdfs://apollo-phx-nn-ha/user/hive/warehouse/mktng.db/incdata_did_to_xid_low_skew_parquet")
   //
   inc_master_df.createOrReplaceTempView("inc_master")
   cguid_to_xid_df.createOrReplaceTempView("cguid")
   did_to_xid_df.createTempView("did")
   //
   //
   //
   //==============
   //UID
   //=============
   spark.sql("select * from sess_cko_bbowa_vi where parent_uid > 0").createOrReplaceTempView("sess_cko_bbowa_vi_non_null_uid")   
   val uid_join_query = s"""
   select 
   A.*,
   incdata_id,
   'uid' as join_strategy
   from  sess_cko_bbowa_vi_non_null_uid A
   inner join inc_master                B
   on A.parent_uid = B.incdata_srcid
   """  
   spark.sql(uid_join_query).createOrReplaceTempView("uid_join")


   //==============
   //DID
   //=============
   val non_null_did_query = """
   select * from sess_cko_bbowa_vi
   where LENGTH(device_id)>=35 
   and device_id not like '%00000000-0000-0000-0000-000000000000%'   
   """
   spark.sql(non_null_did_query).createOrReplaceTempView("sess_cko_bbowa_vi_non_null_did")
   val did_join_query = s"""
   select
   A.*,
   incdata_id,
   'did' as join_strategy
   from sess_cko_bbowa_vi_non_null_did  A
   inner join did B
   on A.device_id = B.ident_attr_value_txt
   """
   spark.sql(did_join_query).createOrReplaceTempView("did_join")
   //==============


   //==========================================================================
   //CGUID
   //==========================================================================
   val cguid_join_query = s"""
   select 
   A.*,
   incdata_id,
   'cguid' as join_strategy
   from sess_cko_bbowa_vi A
   inner join cguid       B
   on A.cguid  =  B.ident_attr_value_txt
   """
   spark.sql(cguid_join_query).createOrReplaceTempView("cguid_join")
   //==========================================================================


   //==========================================================================
   val all_join_sql = """
   select
   coalesce(A.guid,  B.guid, C.guid, D.guid)     as guid,
   coalesce(A.cguid, B.cguid, C.cguid, D.cguid)  as cguid,
   coalesce(A.parent_uid, B.parent_uid, C.parent_uid, D.parent_uid) as parent_uid,
   coalesce(A.session_skey, B.session_skey, C.session_skey, D.session_skey) as session_skey,
   coalesce(A.site_id, B.site_id, C.site_id, D.site_id) as site_id,
   coalesce(A.cobrand, B.cobrand, C.cobrand, D.cobrand) as cobrand,
   coalesce(A.session_start_dt, B.session_start_dt, C.session_start_dt, D.session_start_dt) as session_start_dt,
   coalesce(A.start_timestamp, B.start_timestamp, C.start_timestamp, D.start_timestamp) as start_timestamp,
   coalesce(A.end_timestamp, B.end_timestamp, C.end_timestamp, D.end_timestamp) as end_timestamp,
   coalesce(A.primary_app_id, B.primary_app_id, C.primary_app_id, D.primary_app_id) as primary_app_id,
   coalesce(A.session_traffic_source_id, B.session_traffic_source_id, C.session_traffic_source_id, D.session_traffic_source_id) as session_traffic_source_id,
   coalesce(A.cust_traffic_source_level1, B.cust_traffic_source_level1, C.cust_traffic_source_level1, D.cust_traffic_source_level1) as cust_traffic_source_level1,
   coalesce(A.cust_traffic_source_level2, B.cust_traffic_source_level2, C.cust_traffic_source_level2, D.cust_traffic_source_level2) as cust_traffic_source_level2,
   coalesce(A.traffic_source_level3, B.traffic_source_level3, C.traffic_source_level3, D.traffic_source_level3) as traffic_source_level3,
   coalesce(A.rotation_id, B.rotation_id, C.rotation_id, D.rotation_id) as rotation_id,
   coalesce(A.rvr_id, B.rvr_id, C.rvr_id, D.rvr_id) as rvr_id,
   coalesce(A.idfa, B.idfa, C.idfa, D.idfa)        as idfa,
   coalesce(A.gadid, B.gadid, C.gadid, D.gadid)     as gadid,
   coalesce(A.gdid, B.gdid, C.gdid, D.gdid) as gdid,
   coalesce(A.device_id, B.device_id, C.device_id, D.device_id) as device_id,
   coalesce(A.device_type, B.device_type, C.device_type, D.device_type) as device_type,
   coalesce(A.device_type_level1, B.device_type_level1, C.device_type_level1, D.device_type_level1) as device_type_level1,
   coalesce(A.device_type_level2, B.device_type_level2, C.device_type_level2, D.device_type_level2) as device_type_level2,
   coalesce(A.experience_level1, B.experience_level1, C.experience_level1, D.experience_level1) as experience_level1,
   coalesce(A.experience_level2, B.experience_level2, C.experience_level2, D.experience_level2) as experience_level2,
   coalesce(A.vi_cnt, B.vi_cnt, C.vi_cnt, D.vi_cnt) as vi_cnt,
   coalesce(A.num_txns, B.num_txns, C.num_txns, D.num_txns) as num_txns,
   coalesce(A.gmb_usd, B.gmb_usd, C.gmb_usd, D.gmb_usd) as gmb_usd,
   coalesce(A.has_first_purchase, B.has_first_purchase, C.has_first_purchase, D.has_first_purchase) as has_first_purchase,
   coalesce(A.num_watch, B.num_watch, C.num_watch, D.num_watch) as num_watch ,
   coalesce(A.num_ask, B.num_ask, C.num_ask, D.num_ask) as num_ask,
   coalesce(A.num_ac, B.num_ac, C.num_ac, D.num_ac) as num_ac,
   coalesce(A.num_bo, B.num_bo, C.num_bo, D.num_bo) as num_bo,
   coalesce(A.num_bb, B.num_bb, C.num_bb, D.num_bb) as num_bb,
   coalesce(A.num_bbowa, B.num_bbowa, C.num_bbowa, D.num_bbowa) as num_bbowa,
   coalesce(A.num_meta, B.num_meta, C.num_meta, D.num_meta) as num_meta,
   coalesce(A.num_lv2, B.num_lv2, C.num_lv2, D.num_lv2) as num_lv2,
   coalesce(A.num_lv3, B.num_lv3, C.num_lv3, D.num_lv3) as num_lv3,
   coalesce(A.num_lv4, B.num_lv4, C.num_lv4, D.num_lv4) as num_lv4,
   coalesce(A.specificity, B.specificity, C.specificity, D.specificity) as specificity,
   coalesce(A.specificity_binned, B.specificity_binned, C.specificity_binned, D.specificity_binned) as specificity_binned,
   coalesce(A.n_registrations, B.n_registrations, C.n_registrations, D.n_registrations) as n_registrations,
   coalesce(A.num_fashion,B.num_fashion,C.num_fashion,D.num_fashion) as num_fashion,
   coalesce(A.num_home_garden,B.num_home_garden,C.num_home_garden,D.num_home_garden) as num_home_garden,
   coalesce(A.num_electronics,B.num_electronics,C.num_electronics,D.num_electronics) as num_electronics,
   coalesce(A.num_collectibles,B.num_collectibles,C.num_collectibles,D.num_collectibles) as num_collectibles,
   coalesce(A.num_parts_acc,B.num_parts_acc,C.num_parts_acc,D.num_parts_acc) as num_parts_acc,
   coalesce(A.num_vehicles,B.num_vehicles,C.num_vehicles,D.num_vehicles) as num_vehicles,
   coalesce(A.num_lifestyle,B.num_lifestyle,C.num_lifestyle,D.num_lifestyle) as num_lifestyle,
   coalesce(A.num_bu_industrial,B.num_bu_industrial,C.num_bu_industrial,D.num_bu_industrial) as num_bu_industrial,
   coalesce(A.num_media,B.num_media,C.num_media,D.num_media) as num_media,
   coalesce(A.num_real_estate,B.num_real_estate,C.num_real_estate,D.num_real_estate) as num_real_estate,
   coalesce(A.num_unknown,     B.num_unknown,     C.num_unknown,     D.num_unknown) as num_unknown,     
   coalesce(A.num_clothes_shoes_acc,B.num_clothes_shoes_acc,C.num_clothes_shoes_acc,D.num_clothes_shoes_acc) as num_clothes_shoes_acc,
   coalesce(A.num_home_furniture,B.num_home_furniture,C.num_home_furniture,D.num_home_furniture) as num_home_furniture,
   coalesce(A.num_cars_motors_vehic,B.num_cars_motors_vehic,C.num_cars_motors_vehic,D.num_cars_motors_vehic) as num_cars_motors_vehic,
   coalesce(A.num_cars_p_and_acc,B.num_cars_p_and_acc,C.num_cars_p_and_acc,D.num_cars_p_and_acc) as num_cars_p_and_acc,
   coalesce(A.num_sporting_goods,B.num_sporting_goods,C.num_sporting_goods,D.num_sporting_goods) as num_sporting_goods,
   coalesce(A.num_toys_games,B.num_toys_games,C.num_toys_games,D.num_toys_games) as num_toys_games,
   coalesce(A.num_health_beauty,B.num_health_beauty,C.num_health_beauty,D.num_health_beauty) as num_health_beauty,
   coalesce(A.num_mob_phones,B.num_mob_phones,C.num_mob_phones,D.num_mob_phones) as num_mob_phones,
   coalesce(A.num_business_ind,B.num_business_ind,C.num_business_ind,D.num_business_ind) as num_business_ind,
   coalesce(A.num_baby,B.num_baby,C.num_baby,D.num_baby) as num_baby,
   coalesce(A.num_jewellry_watches,B.num_jewellry_watches,C.num_jewellry_watches,D.num_jewellry_watches) as num_jewellry_watches,
   coalesce(A.num_computers,B.num_computers,C.num_computers,D.num_computers) as num_computers,
   coalesce(A.num_modellbau,B.num_modellbau,C.num_modellbau,D.num_modellbau) as num_modellbau,
   coalesce(A.num_antiquities,B.num_antiquities,C.num_antiquities,D.num_antiquities) as num_antiquities,
   coalesce(A.num_sound_vision,B.num_sound_vision,C.num_sound_vision,D.num_sound_vision) as num_sound_vision,
   coalesce(A.num_video_games_consoles,B.num_video_games_consoles,C.num_video_games_consoles,D.num_video_games_consoles) as num_video_games_consoles,

   coalesce(A.join_strategy, B.join_strategy, C.join_strategy, 'no_join') as join_strategy,
   coalesce(A.incdata_id, B.incdata_id, C.incdata_id) as incdata_id,
   coalesce(A.dt, B.dt, C.dt, D.dt) as dt

   from              uid_join A
   full outer join   did_join            B
   on                A.guid            = B.guid
   and               A.session_skey    = B.session_skey
   full outer join   cguid_join          C
   on                A.guid            = C.guid
   and               A.session_skey    = C.session_skey
   full outer join   sess_cko_bbowa_vi   D
   on                A.guid            = D.guid
   and               A.session_skey    = D.session_skey   
   """

   val rowNumb = (row_number()
		.over(Window.partitionBy(col("guid"), col("session_skey"), col("site_id"), col("cobrand"), col("incdata_id"))
		      .orderBy(col("device_type_level1"), col("device_type_level2"),col("experience_level1"),col("experience_level2"))
			)
              )
    
   val sess_cko_bbowa_vi_xid_df = (spark
			       .sql(all_join_sql)
			       .select($"*", rowNumb as "rnum")
			       .filter($"rnum"===1)
			       )
   
    (sess_cko_bbowa_vi_xid_df
   .write
   .partitionBy("dt")
   .mode(SaveMode.Append)
   .save("hdfs://apollo-phx-nn-ha/user/hive/warehouse/mktng.db/sess_cko_bbowa_vi_with_xid")
  )   
 }
}
