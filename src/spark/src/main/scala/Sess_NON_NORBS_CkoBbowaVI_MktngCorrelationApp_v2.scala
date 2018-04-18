import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/* ***************************


/apache/spark-2.1.1-bin-hadoop2.7/bin/spark-submit --class Sess_NON_NORBS_CkoBbowaVI_MktngCorrelationApp_v2 --master yarn --deploy-mode cluster \
--conf spark.yarn.am.extraJavaOptions="-Dhdp.version=2.4.2.0-258" --conf spark.driver.extraJavaOptions="-Dhdp.version=2.4.2.0-258" \
  --conf spark.yarn.access.namenodes=hdfs://apollo-phx-nn-ha  --queue hddq-other-fin  \
  --conf spark.yarn.appMasterEnv.SPARK_HOME=/apache/spark-2.1.1-bin-hadoop2.7/bin/spark-submit --num-executors 1000 \
  --driver-memory 30g --executor-memory 30g /tmp/correlation_2.11-1.0.jar "2018-04-14" "2018-04-14" "2018-04-14" "true"

 *************************** */


object Sess_NON_NORBS_CkoBbowaVI_MktngCorrelationApp_v2 {

  def main(args: Array[String]): Unit = {

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
    val mktng_start_dt  = args(2)
    val norb = args(3).toBoolean // boolean variable
    val mktng_end_dt    = sess_end_dt

    // ========================


    // ========================
    // M EVENTS
    // ========================
    val mevents_df   = spark.read.parquet("hdfs://apollo-phx-nn-ha/apps/hdmi-technology/b_marketing_ep_infr/production/MARKETING_EVENT/MARKETING_EVENT/0/0/")

    // ========================
    // SESSIONS
    // ========================
    val sess_xid_df = spark.read.parquet("hdfs://apollo-phx-nn-ha/user/hive/warehouse/mktng.db/sessions_data")


    // ======================================
    //FILTER FOR DATES AND NORB OR NON-NORBS
    // ======================================
    val sess_df   = if (norb) {
      sess_xid_df.filter(s"dt between '$sess_start_dt' and '$sess_end_dt' and has_first_purchase = 0").sample(false, 0.20)
    }
    else{
      sess_xid_df.filter(s"dt between '$sess_start_dt' and '$sess_end_dt' and has_first_purchase > 0")
    }

    sess_df.cache()
    println("the count of transactions is " + sess_df.count())

    //TAKE ONLY CLICKS
    val mktng_df = mevents_df.filter(s"dt between '$mktng_start_dt' and '$mktng_end_dt' and event_type_id in (1,5,8)")

    sess_df.createOrReplaceTempView("sess_xid")
    mktng_df.createOrReplaceTempView("mktng")


    // ========================

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
   A.device_id,
   A.experience_level1,
   A.experience_level2,
   A.device_type_level1,
   A.device_type_level2,
   A.flex_field_map,

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
   B.lndg_page_id as sess_lndg_page_id,
   B.exit_page_id as sess_exit_page_id,
   B.valid_page_count as sess_valid_page_count,
   B.gr_cnt as sess_gr_cnt,
   B.gr_1_cnt as sess_gr_1_cnt,
   B.vi_cnt as sess_vi_cnt,
   B.homepage_cnt as sess_homepage_cnt,
   B.myebay_cnt as sess_myebay_cnt,
   B.signin_cnt as sess_signin_cnt,
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
   B.num_unknown  as sess_num_unknown,
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

   (unix_timestamp(B.start_timestamp) - unix_timestamp(A.event_ts))/86400. as day_diff,
   (unix_timestamp(B.start_timestamp) - unix_timestamp(A.event_ts))        as sec_diff,
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



  sql(common_join_sql+cguid_query_tail).createOrReplaceTempView("cguid_join")
//  sql(common_join_sql+  did_query_tail).createOrReplaceTempView("did_join")
    }
}
