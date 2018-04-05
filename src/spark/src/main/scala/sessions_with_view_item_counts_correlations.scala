import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object Sessions_with_VI_counts_APP {

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


    //=========================
    //VIEW ITEMS 
    //=========================
    val sess_start_dt   = args(0)
    val sess_end_dt     = args(1)
    val mktng_start_dt  = args(2)
    val mktng_end_dt    = sess_end_dt
    // ========================


    // ========================
    // SESSION EVENTS
    // ========================
    val mktng_path   = s"hdfs://apollo-phx-nn-ha/user/hive/warehouse/mktng.db/sess_with_item_counts_with_xid"
       
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
    //FILTER FOR DATES
    // ========================
    //ONLY NON NORB TRANSACTIONS!
    val sess_df   = sess_xid_df.filter(s"dt between '$sess_start_dt' and '$sess_end_dt'")//.sample(false, 0.20)


    sess_df.createOrReplaceTempView("sess_xid")
    mktng_df.createOrReplaceTempView("sess_vi_count")


    val common_join_sql = """
    A.guid,
    A.cguid,
    A.session_skey,
    A.session_start_dt,
    A.site_id,
    A.user_id,
    A.incdata_id,
    B.guid as vi_guid,
    B.cguid as vi_cguid,
    B.session_skey as vi_session_skey,
    B.site_id as vi_site_id,
    B.session_start_dt as vi_session_start_dt,
    B.user_id as vi_user_id,
    B.session_traffic_source_id as vi_session_traffic_source_id,
    B.num_fashion as vi_num_fashion,
    B.num_home_garden as vi_num_home_garden,
    B.num_electronics as vi_num_electronics,
    B.num_collectibles as vi_num_collectibles,
    B.num_parts_acc as vi_num_parts_acc,
    B.num_vehicles as vi_num_vehicles,
    B.num_lifestyle as vi_num_lifestyle,
    B.num_bu_industrial as vi_num_bu_industrial,
    B.num_media as vi_num_media,
    B.num_real_estate as vi_num_real_estate,
    B.num_unknown as vi_num_unknown,
    B.num_clothes_shoes_acc as vi_num_clothes_shoes_acc,
    B.num_home_furniture as vi_num_home_furniture,
    B.num_cars_motors_vehic as vi_num_cars_motors_vehic,
    B.num_cars_p_and_acc as vi_num_cars_p_and_acc,
    B.num_sporting_goods as vi_num_sporting_goods,
    B.num_toys_games as vi_num_toys_games,
    B.num_health_beauty as vi_num_health_beauty,
    B.num_mob_phones as vi_num_mob_phones,
    B.num_business_ind as vi_num_business_ind,
    B.num_baby as vi_num_baby,
    B.num_jewellry_watches as vi_num_jewellry_watches,
    B.num_computers as vi_num_computers,
    B.num_modellbau as vi_num_modellbau,
    B.num_antiquities as vi_num_antiquities,
    B.num_sound_vision as vi_num_sound_vision,
    B.num_video_games_consoles as vi_num_video_games_consoles,
    B.incdata_id as vi_incdata_id,
    """




   sql("select * from sess_vi_count  where user_id >0").createOrReplaceTempView("sess_vi_count_non_null_uid")
   sql("select * from sess_xid where parent_uid >0").createOrReplaceTempView("sess_xid_non_null_uid")

    val uid_query_tail = """
    "uid"      as join_strategy

    FROM        sess_vi_count_non_null_uid    A
    INNER JOIN  sess_xid_non_null_uid         B
    on          A.user_id = B.parent_uid

    where (A.session_start_dt   <= B.session_start_dt)
    and  (datediff(B.session_start_dt, A.session_start_dt) <= 14)
    and  (unix_timestamp(A.start_timestamp ) < unix_timestamp(B.start_timestamp))
    and  (unix_timestamp(B.start_timestamp) -   unix_timestamp(A.start_timestamp) <= 14*86400)   
    """
    spark.sql(uid_join_query).createOrReplaceTempView("uid_join")


    val cguid_query_tail = """
    "cguid"     as join_strategy

    FROM           sess_vi_count   A
    INNER JOIN     sess_xid        B
    on             A.cguid  = B.cguid

    where (A.session_start_dt   <= B.session_start_dt)
    and  (datediff(B.session_start_dt, A.session_start_dt) <= 14)
    and  (unix_timestamp(A.start_timestamp ) < unix_timestamp(B.start_timestamp))
    and  (unix_timestamp(B.start_timestamp) - unix_timestamp(A.start_timestamp) <= 14*86400)
    """
    spark.sql(cguid_join_query).createOrReplaceTempView("cguid_join")


    val xid_query_tail = """
    "xid"     as join_strategy

    FROM           sess_vi_count   A
    INNER JOIN     sess_xid        B
    on             A.incdata_id  = B.incdata_id

    where (A.session_start_dt   <= B.session_start_dt)
    and  (datediff(B.session_start_dt, A.session_start_dt) <= 14)
    and  (unix_timestamp(A.start_timestamp ) < unix_timestamp(B.start_timestamp))
    and  (unix_timestamp(B.start_timestamp) - unix_timestamp(A.start_timestamp) <= 14*86400)
    """
    

    


    val full_join_sql = """
    select
    coalesce(A.guid,B.guid,C.guid,D.guid) as guid,
    coalesce(A.cguid,B.cguid,C.cguid,D.cguid) as cguid,
    coalesce(A.session_skey,B.session_skey,C.session_skey,D.session_skey) as session_skey,
    coalesce(A.session_start_dt,B.session_start_dt,C.session_start_dt,D.session_start_dt) as session_start_dt,
    coalesce(A.site_id,B.site_id,C.site_id,D.site_id) as site_id,
    coalesce(A.user_id,B.user_id,C.user_id,D.user_id) as user_id,
    coalesce(A.incdata_id,B.incdata_id,C.incdata_id,D.incdata_id) as incdata_id,
    coalesce(A.guid as vi_guid,B.guid as vi_guid,C.guid as vi_guid,D.guid as vi_guid) as guid as vi_guid,
    coalesce(A.cguid as vi_cguid,B.cguid as vi_cguid,C.cguid as vi_cguid,D.cguid as vi_cguid) as cguid as vi_cguid,
    coalesce(A.vi_session_skey,B.vi_session_skey,C.vi_session_skey,D.vi_session_skey) as vi_session_skey,
    coalesce(A.vi_site_id,B.vi_site_id,C.vi_site_id,D.vi_site_id) as vi_site_id,
    coalesce(A.vi_session_start_dt,B.vi_session_start_dt,C.vi_session_start_dt,D.vi_session_start_dt) as vi_session_start_dt,
    coalesce(A.vi_user_id,B.vi_user_id,C.vi_user_id,D.vi_user_id) as vi_user_id,
    coalesce(A.vi_session_traffic_source_id,B.vi_session_traffic_source_id,C.vi_session_traffic_source_id,D.vi_session_traffic_source_id) as vi_session_traffic_source_id,
    coalesce(A.vi_num_fashion,B.vi_num_fashion,C.vi_num_fashion,D.vi_num_fashion) as vi_num_fashion,
    coalesce(A.vi_num_home_garden,B.vi_num_home_garden,C.vi_num_home_garden,D.vi_num_home_garden) as vi_num_home_garden,
    coalesce(A.vi_num_electronics,B.vi_num_electronics,C.vi_num_electronics,D.vi_num_electronics) as vi_num_electronics,
    coalesce(A.vi_num_collectibles,B.vi_num_collectibles,C.vi_num_collectibles,D.vi_num_collectibles) as vi_num_collectibles,
    coalesce(A.vi_num_parts_acc,B.vi_num_parts_acc,C.vi_num_parts_acc,D.vi_num_parts_acc) as vi_num_parts_acc,
    coalesce(A.vi_num_vehicles,B.vi_num_vehicles,C.vi_num_vehicles,D.vi_num_vehicles) as vi_num_vehicles,
    coalesce(A.vi_num_lifestyle,B.vi_num_lifestyle,C.vi_num_lifestyle,D.vi_num_lifestyle) as vi_num_lifestyle,
    coalesce(A.vi_num_bu_industrial,B.vi_num_bu_industrial,C.vi_num_bu_industrial,D.vi_num_bu_industrial) as vi_num_bu_industrial,
    coalesce(A.vi_num_media,B.vi_num_media,C.vi_num_media,D.vi_num_media) as vi_num_media,
    coalesce(A.vi_num_real_estate,B.vi_num_real_estate,C.vi_num_real_estate,D.vi_num_real_estate) as vi_num_real_estate,
    coalesce(A.vi_num_unknown,B.vi_num_unknown,C.vi_num_unknown,D.vi_num_unknown) as vi_num_unknown,
    coalesce(A.vi_num_clothes_shoes_acc,B.vi_num_clothes_shoes_acc,C.vi_num_clothes_shoes_acc,D.vi_num_clothes_shoes_acc) as vi_num_clothes_shoes_acc,
    coalesce(A.vi_num_home_furniture,B.vi_num_home_furniture,C.vi_num_home_furniture,D.vi_num_home_furniture) as vi_num_home_furniture,
    coalesce(A.vi_num_cars_motors_vehic,B.vi_num_cars_motors_vehic,C.vi_num_cars_motors_vehic,D.vi_num_cars_motors_vehic) as vi_num_cars_motors_vehic,
    coalesce(A.vi_num_cars_p_and_acc,B.vi_num_cars_p_and_acc,C.vi_num_cars_p_and_acc,D.vi_num_cars_p_and_acc) as vi_num_cars_p_and_acc,
    coalesce(A.vi_num_sporting_goods,B.vi_num_sporting_goods,C.vi_num_sporting_goods,D.vi_num_sporting_goods) as vi_num_sporting_goods,
    coalesce(A.vi_num_toys_games,B.vi_num_toys_games,C.vi_num_toys_games,D.vi_num_toys_games) as vi_num_toys_games,
    coalesce(A.vi_num_health_beauty,B.vi_num_health_beauty,C.vi_num_health_beauty,D.vi_num_health_beauty) as vi_num_health_beauty,
    coalesce(A.vi_num_mob_phones,B.vi_num_mob_phones,C.vi_num_mob_phones,D.vi_num_mob_phones) as vi_num_mob_phones,
    coalesce(A.vi_num_business_ind,B.vi_num_business_ind,C.vi_num_business_ind,D.vi_num_business_ind) as vi_num_business_ind,
    coalesce(A.vi_num_baby,B.vi_num_baby,C.vi_num_baby,D.vi_num_baby) as vi_num_baby,
    coalesce(A.vi_num_jewellry_watches,B.vi_num_jewellry_watches,C.vi_num_jewellry_watches,D.vi_num_jewellry_watches) as vi_num_jewellry_watches,
    coalesce(A.vi_num_computers,B.vi_num_computers,C.vi_num_computers,D.vi_num_computers) as vi_num_computers,
    coalesce(A.vi_num_modellbau,B.vi_num_modellbau,C.vi_num_modellbau,D.vi_num_modellbau) as vi_num_modellbau,
    coalesce(A.vi_num_antiquities,B.vi_num_antiquities,C.vi_num_antiquities,D.vi_num_antiquities) as vi_num_antiquities,
    coalesce(A.vi_num_sound_vision,B.vi_num_sound_vision,C.vi_num_sound_vision,D.vi_num_sound_vision) as vi_num_sound_vision,
    coalesce(A.vi_num_video_games_consoles,B.vi_num_video_games_consoles,C.vi_num_video_games_consoles,D.vi_num_video_games_consoles) as vi_num_video_games_consoles,
    coalesce(A.vi_incdata_id,B.vi_incdata_id,C.vi_incdata_id,D.vi_incdata_id) as vi_incdata_id,
    coalesce(A.join_strategy,B.join_strategy,C.join_strategy,"no_join") as join_strategy,
    coalesce(A.dt, B.dt, C.dt, D.dt) as dt

    from 

    """
    


    
  }
}
