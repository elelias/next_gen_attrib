import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.sql.Date

/* ***************************


/apache/spark-2.1.1-bin-hadoop2.7/bin/spark-submit --class Sess_NON_NORBS_CkoBbowaVI_MktngCorrelationApp_v2 --master yarn --deploy-mode cluster \
--conf spark.yarn.am.extraJavaOptions="-Dhdp.version=2.4.2.0-258" --conf spark.driver.extraJavaOptions="-Dhdp.version=2.4.2.0-258" \
  --conf spark.yarn.access.namenodes=hdfs://apollo-phx-nn-ha  --queue hddq-other-fin  \
  --conf spark.yarn.appMasterEnv.SPARK_HOME=/apache/spark-2.1.1-bin-hadoop2.7/bin/spark-submit --num-executors 1000 \
  --driver-memory 30g --executor-memory 30g /tmp/correlation_2.11-1.0.jar "2018-04-14" "2018-04-14" "2018-04-06" "norb"


When running in Spades from Apollo :

/home/eron/anaconda2/bin/python -c "import send_spark as ss; send_template = '''/usr/hdp/current/spark-2.1.0-bin-hadoop2.7/bin/spark-submit  --class Sess_NON_NORBS_CkoBbowaVI_MktngCorrelationApp_v2 --master yarn --deploy-mode cluster  --conf spark.yarn.am.extraJavaOptions="-Dhdp.version=2.4.2.0-258" --conf spark.driver.extraJavaOptions="-Dhdp.version=2.4.2.0-258" --conf spark.yarn.access.namenodes=hdfs://apollo-phx-nn-ha  --queue hddq-other-fin  --conf spark.yarn.appMasterEnv.SPARK_HOME=/usr/hdp/current/spark-2.1.0-bin-hadoop2.7/bin/spark-submit --num-executors 1000 --driver-memory 30g --executor-memory 30g /home/adeabreu/session_mktng_corr_2.11-1.0.jar "2018-04-15" "2018-04-15" "2018-04-15" "norb" '''; stdin, stdout, stderr= ss.run_spark_on_spades(send_template)"




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


    import spark.implicits._
    import spark.sql
    import org.apache.spark.sql.SaveMode


    val sess_start_dt   = args(0)
    val sess_end_dt     = args(1)
    val mktng_start_dt  = args(2)
    val session_type = args(3)
    val mktng_end_dt    = sess_end_dt

    //To avoid appending on the same file, first check if the file exist
//    val hadoopfs: FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)
//
//    if (!testDirExist(s"hdfs://apollo-phx-nn-ha/user/hive/warehouse/mktng.db/sess_mktng_corr/session_type=$session_type/sess_dt=$sess_start_dt")(hadoopfs))
//      throw new Exception(s"The file for a $session_type session and date $sess_start_dt already exist!")


//    val mktngDates = Seq()
//    val results = mktngDates.
//      map(mktngDate => spark.read.parquet(s"hdfs://apollo-phx-nn-ha/apps/hdmi-technology/b_marketing_ep_infr/production/MARKETING_EVENT/MARKETING_EVENT/0/0/dt=$mktngDate")).
//      reduce(_.union(_))



    // ========================
    // M EVENTS
    // ========================
    val mevents_df   = spark.read.parquet(s"hdfs://apollo-phx-nn-ha/apps/hdmi-technology/b_marketing_ep_infr/production/MARKETING_EVENT/MARKETING_EVENT/0/0/")

    // ========================
    // SESSIONS
    // ========================
    val sess_df_all = spark.read.parquet("hdfs://apollo-phx-nn-ha/user/hive/warehouse/mktng.db/sessions_data")

    // =================================================
    // XID - only needed to get the anonymous sessions
    // =================================================
    val cguid_df = spark.read.parquet("hdfs://apollo-phx-nn-ha/user/hive/warehouse/mktng.db/incdata_cguid_to_xid_count_parquet")

    // =====================================================
    // DeviceID - only needed to get the anonymous sessions
    // =====================================================
    val did_df = spark.read.parquet("hdfs://apollo-phx-nn-ha/user/hive/warehouse/mktng.db/incdata_did_to_xid_count_parquet")

    // ======================================
    // FILTER DATES & MKTNG CLICKS
    // ======================================

    val sess_df_all_date = sess_df_all
      .where(($"dt" >= sess_start_dt) && ($"dt" <= sess_end_dt))

    val mktng_df = mevents_df
//      .where(($"dt" >= "2018-04-20") && ($"dt" <= "2018-04-20"))
      .where(($"dt" >= mktng_start_dt) && ($"dt" <= mktng_end_dt))
      .where(("event_type_id in (1,5,8)"))

//    val mktng_df = mevents_df.filter(s"dt between '$mktng_start_dt' and '$mktng_end_dt' and event_type_id in (1,5,8)")


    // ==========================================
    // GET NORBS NON-NORBS OR ANONYMOUS SESSIONS
    // ==========================================

    val sess_df   = session_type match {

      case "nonnorb" => sess_df_all_date
        .filter($"has_first_purchase" === 0)
        .sample(false, 0.20)

      case "norb" => sess_df_all_date
        .filter($"has_first_purchase" > 0)

      case "anonymous" => {

        val sess_cguid_anon_df = sess_df_all_date
          .alias("lft")
          .join(cguid_df, Seq("cguid"), "leftanti")
          .select("lft.*")

        // device_id ="0...0" is excluded for join computational complexity reasons
        val sess_df_all_did_filtered = sess_cguid_anon_df
          .where((length($"device_id") >= 35) && ($"device_id" != "%00000000-0000-0000-0000-000000000000%"))

        val sub_non_zero_sess_did_anon_df = sess_df_all_did_filtered
          .alias("lft")
          .join(did_df, $"device_id"===$"did", "leftanti")
          .select("lft.*")

        // We need to add back sessions with the device_id ="0...0" that were in sess_cguid_anon_df, and they are not in did_df!
        val sub_zero_sess_did_anon_df = sess_cguid_anon_df
          .where(($"device_id" === "%00000000-0000-0000-0000-000000000000%"))

        sub_non_zero_sess_did_anon_df
          .union(sub_zero_sess_did_anon_df)
          .dropDuplicates() //There shouldn't be duplicates!

      }
      case _ => throw new IllegalArgumentException("Last argument should be: 'norb', 'nonnorb' or 'anonymous' ")

    }


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


  sql(common_join_sql + cguid_query_tail).createOrReplaceTempView("cguid_join")
  sql(common_join_sql +  did_query_tail).createOrReplaceTempView("did_join")


    val full_join_sql = """
   select
   coalesce(B.guid,C.guid) as guid,
   coalesce(B.cguid,C.cguid) as cguid,
   coalesce(B.user_id,C.user_id) as user_id,
   coalesce(B.event_id,C.event_id) as event_id,
   coalesce(B.event_id_type,C.event_id_type) as event_id_type,
   coalesce(B.event_type_id,C.event_type_id) as event_type_id,
   coalesce(B.channel_id,C.channel_id) as channel_id,
   coalesce(B.channel_name,C.channel_name) as channel_name,
   coalesce(B.rotation_id,C.rotation_id) as rotation_id,
   coalesce(B.rotation_name,C.rotation_name) as rotation_name,
   coalesce(B.campaign_id,C.campaign_id) as campaign_id,
   coalesce(B.campaign_name,C.campaign_name) as campaign_name,
   coalesce(B.event_dt,C.event_dt) as event_dt,
   coalesce(B.event_ts,C.event_ts) as event_ts,
   coalesce(B.device_id,C.device_id) as device_id,
   coalesce(B.experience_level1,C.experience_level1) as experience_level1,
   coalesce(B.experience_level2,C.experience_level2) as experience_level2,
   coalesce(B.device_type_level1,C.device_type_level1) as device_type_level1,
   coalesce(B.device_type_level2,C.device_type_level2) as device_type_level2,
   coalesce(B.flex_field_map,C.flex_field_map) as flex_field_map,
   
   coalesce(B.sess_guid,C.sess_guid) as sess_guid,
   coalesce(B.sess_cguid,C.sess_cguid) as sess_cguid,
   coalesce(B.sess_parent_uid,C.sess_parent_uid) as sess_parent_uid,
   coalesce(B.sess_session_skey,C.sess_session_skey) as sess_session_skey,
   coalesce(B.sess_site_id,C.sess_site_id) as sess_site_id,
   coalesce(B.sess_cobrand,C.sess_cobrand) as sess_cobrand,
   coalesce(B.sess_session_start_dt,C.sess_session_start_dt) as sess_session_start_dt,
   coalesce(B.sess_start_timestamp,C.sess_start_timestamp) as sess_start_timestamp,
   coalesce(B.sess_end_timestamp,C.sess_end_timestamp) as sess_end_timestamp,
   coalesce(B.sess_primary_app_id,C.sess_primary_app_id) as sess_primary_app_id,
   coalesce(B.sess_session_traffic_source_id,C.sess_session_traffic_source_id) as sess_session_traffic_source_id,
   coalesce(B.sess_cust_traffic_source_level1,C.sess_cust_traffic_source_level1) as sess_cust_traffic_source_level1,
   coalesce(B.sess_cust_traffic_source_level2,C.sess_cust_traffic_source_level2) as sess_cust_traffic_source_level2,
   coalesce(B.sess_traffic_source_level3,C.sess_traffic_source_level3) as sess_traffic_source_level3,
   coalesce(B.sess_rotation_id,C.sess_rotation_id) as sess_rotation_id,
   coalesce(B.sess_rvr_id,C.sess_rvr_id) as sess_rvr_id,
   coalesce(B.sess_idfa,C.sess_idfa) as sess_idfa,
   coalesce(B.sess_gadid,C.sess_gadid) as sess_gadid,
   coalesce(B.sess_gdid,C.sess_gdid) as sess_gdid,
   coalesce(B.sess_device_id,C.sess_device_id) as sess_device_id,
   coalesce(B.sess_device_type,C.sess_device_type) as sess_device_type,
   coalesce(B.sess_device_type_level1,C.sess_device_type_level1) as sess_device_type_level1,
   coalesce(B.sess_device_type_level2,C.sess_device_type_level2) as sess_device_type_level2,
   coalesce(B.sess_experience_level1,C.sess_experience_level1) as sess_experience_level1,
   coalesce(B.sess_experience_level2,C.sess_experience_level2) as sess_experience_level2,
   coalesce(B.sess_lndg_page_id,C.sess_lndg_page_id) as sess_lndg_page_id,
   coalesce(B.sess_exit_page_id,C.sess_exit_page_id) as sess_exit_page_id,
   coalesce(B.sess_valid_page_count,C.sess_valid_page_count) as sess_valid_page_count,
   coalesce(B.sess_gr_cnt,C.sess_gr_cnt) as sess_gr_cnt,
   coalesce(B.sess_gr_1_cnt,C.sess_gr_1_cnt) as sess_gr_1_cnt,
   coalesce(B.sess_vi_cnt,C.sess_vi_cnt)                     as sess_vi_cnt,
   coalesce(B.sess_homepage_cnt,C.sess_homepage_cnt)                     as sess_homepage_cnt,
   coalesce(B.sess_myebay_cnt,C.sess_myebay_cnt)                     as sess_myebay_cnt,
   coalesce(B.sess_signin_cnt,C.sess_signin_cnt)                     as sess_signin_cnt,
   coalesce(B.sess_num_txns,C.sess_num_txns)             as sess_num_txns,
   coalesce(B.sess_gmb_usd,C.sess_gmb_usd)                 as sess_gmb_usd,
   coalesce(B.sess_has_first_purchase,C.sess_has_first_purchase) as sess_has_first_purchase,
   coalesce(B.sess_num_watch,C.sess_num_watch)         as sess_num_watch,
   coalesce(B.sess_num_ask,C.sess_num_ask)                 as sess_num_ask,
   coalesce(B.sess_num_ac,C.sess_num_ac)                     as sess_num_ac,
   coalesce(B.sess_num_bo,C.sess_num_bo)                     as sess_num_bo,
   coalesce(B.sess_num_bb,C.sess_num_bb)                     as sess_num_bb,
   coalesce(B.sess_num_bbowa,C.sess_num_bbowa)         as sess_num_bbowa,
   coalesce(B.sess_num_meta,C.sess_num_meta)             as sess_num_meta,
   coalesce(B.sess_num_lv2,C.sess_num_lv2)                 as sess_num_lv2,
   coalesce(B.sess_num_lv3,C.sess_num_lv3)                 as sess_num_lv3,
   coalesce(B.sess_num_lv4,C.sess_num_lv4)                 as sess_num_lv4,
   coalesce(B.sess_specificity,C.sess_specificity) as sess_specificity,
   coalesce(B.sess_specificity_binned,C.sess_specificity_binned) as sess_specificity_binned,
   coalesce(B.sess_num_fashion,C.sess_num_fashion) as sess_num_fashion,
   coalesce(B.sess_num_home_garden,C.sess_num_home_garden) as sess_num_home_garden,
   coalesce(B.sess_num_electronics,C.sess_num_electronics) as sess_num_electronics,
   coalesce(B.sess_num_collectibles,C.sess_num_collectibles) as sess_num_collectibles,
   coalesce(B.sess_num_parts_acc,C.sess_num_parts_acc) as sess_num_parts_acc,
   coalesce(B.sess_num_vehicles,C.sess_num_vehicles) as sess_num_vehicles,
   coalesce(B.sess_num_lifestyle,C.sess_num_lifestyle) as sess_num_lifestyle,
   coalesce(B.sess_num_bu_industrial,C.sess_num_bu_industrial) as sess_num_bu_industrial,
   coalesce(B.sess_num_media,C.sess_num_media) as sess_num_media,
   coalesce(B.sess_num_real_estate,C.sess_num_real_estate) as sess_num_real_estate,
   coalesce(B.sess_num_unknown,     C.sess_num_unknown) as sess_num_unknown,
   coalesce(B.sess_num_clothes_shoes_acc,C.sess_num_clothes_shoes_acc) as sess_num_clothes_shoes_acc,
   coalesce(B.sess_num_home_furniture,C.sess_num_home_furniture) as sess_num_home_furniture,
   coalesce(B.sess_num_cars_motors_vehic,C.sess_num_cars_motors_vehic) as sess_num_cars_motors_vehic,
   coalesce(B.sess_num_cars_p_and_acc,C.sess_num_cars_p_and_acc) as sess_num_cars_p_and_acc,
   coalesce(B.sess_num_sporting_goods,C.sess_num_sporting_goods) as sess_num_sporting_goods,
   coalesce(B.sess_num_toys_games,C.sess_num_toys_games) as sess_num_toys_games,
   coalesce(B.sess_num_health_beauty,C.sess_num_health_beauty) as sess_num_health_beauty,
   coalesce(B.sess_num_mob_phones,C.sess_num_mob_phones) as sess_num_mob_phones,
   coalesce(B.sess_num_business_ind,C.sess_num_business_ind) as sess_num_business_ind,
   coalesce(B.sess_num_baby,C.sess_num_baby) as sess_num_baby,
   coalesce(B.sess_num_jewellry_watches,C.sess_num_jewellry_watches) as sess_num_jewellry_watches,
   coalesce(B.sess_num_computers,C.sess_num_computers) as sess_num_computers,
   coalesce(B.sess_num_modellbau,C.sess_num_modellbau) as sess_num_modellbau,
   coalesce(B.sess_num_antiquities,C.sess_num_antiquities) as sess_num_antiquities,
   coalesce(B.sess_num_sound_vision,C.sess_num_sound_vision) as sess_num_sound_vision,
   coalesce(B.sess_num_video_games_consoles, C.sess_num_video_games_consoles) as sess_num_video_games_consoles,

   coalesce(B.day_diff,C.day_diff) as day_diff,
   coalesce(B.sec_diff,C.sec_diff) as sec_diff,
   coalesce(B.mktng_join_strategy,C.mktng_join_strategy) as mktng_join_strategy,
   coalesce(B.sess_session_start_dt, C.sess_session_start_dt) as sess_dt,
   coalesce(B.event_dt, C.event_dt) as mktng_dt
   FROM             did_join                B
   FULL OUTER JOIN  cguid_join              C
   ON               B.event_id           =  C.event_id
   AND              B.sess_guid          =  C.sess_guid
   AND              B.sess_session_skey  =  C.sess_session_skey

   """

    val rNumb = row_number()
      .over(Window.partitionBy(col("sess_guid"), col("sess_session_skey"), col("event_id"))
        .orderBy(col("device_type_level1"), col("device_type_level2"), col("experience_level1"), col("experience_level2"))
      )

    val full_join_df = spark
      .sql(full_join_sql)
      .withColumn("rnum", rNumb)
      .filter($"rnum"===1)

    full_join_df
      .withColumn("session_type", lit(session_type))
      .write
      .partitionBy("session_type", "sess_dt", "mktng_dt")
      .mode(SaveMode.Append)
      .save("hdfs://apollo-phx-nn-ha/user/hive/warehouse/mktng.db/sess_mktng_corr")

    }

//  def testDirExist(path: String)(hadoopfs: FileSystem): Boolean = {
//      val p = new Path(path)
//       hadoopfs.exists(p) && hadoopfs.getFileStatus(p).isDirectory
//  }
}
