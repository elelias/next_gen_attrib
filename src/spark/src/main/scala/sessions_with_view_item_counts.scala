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
    val items_path = s"hdfs://apollo-phx-nn-ha/user/hive/warehouse/mktng.db/view_items_cats_parquet_external_ana"

    spark.read.load(items_path).createOrReplaceTempView("items_all")
    val items_sql = s"""
    select
    guid,
    cguid,
    session_skey,
    site_id,
    session_start_dt,
    coalesce(user_id, 0) as user_id,
    session_traffic_source_id,
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
    cguid,
    session_skey,
    site_id,
    session_start_dt,
    coalesce(user_id, 0),
    session_traffic_source_id
    """
    val session_items_df = spark.sql(items_sql)
    
    

    

    (session_items_df
     .withColumn("dt",$"session_start_dt")
     .write
     .partitionBy("dt")
     .mode(SaveMode.Append)
     .save("hdfs://apollo-phx-nn-ha/user/hive/warehouse/mktng.db/sess_with_item_counts")
   )
    

    
    //==========================================================================
    // JOIN TO XID
    //==========================================================================
    //
    //
    val sessions_all_df = spark.read.load(s"hdfs://apollo-phx-nn-ha/user/hive/warehouse/mktng.db/sess_with_item_counts")
    sessions_all_df.createOrReplaceTempView("sessions_all")
    spark.sql(s"select * from sessions_all where dt between '${sess_start_dt}' and '${sess_end_dt}' ").createOrReplaceTempView("sessions")
    //===========================


    //===========================
    // XID 
    //===========================
    val inc_master_df   = spark.read.load("hdfs://apollo-phx-nn-ha/user/hive/warehouse/mktng.db/inc_master_low_skew_parquet")
    val cguid_to_xid_df = spark.read.load("hdfs://apollo-phx-nn-ha/user/hive/warehouse/mktng.db/incdata_cguid_to_xid_low_skew_parquet")
    //===========================

    //
    //===========================
    inc_master_df.createOrReplaceTempView("inc_master")
    cguid_to_xid_df.createOrReplaceTempView("cguid")
    //
    //
    //
    //==============
    //UID
    //=============
    spark.sql("select * from sessions where user_id > 0").createOrReplaceTempView("sessions_non_null_uid")   
    val uid_join_query = s"""
    select 
    A.*,
    incdata_id,
    'uid' as join_strategy
    from  sessions_non_null_uid A
    inner join inc_master                B
    on A.user_id = B.incdata_srcid
    """  
    spark.sql(uid_join_query).createOrReplaceTempView("uid_join")



    //==========================================================================
    //CGUID
    //==========================================================================
    val cguid_join_query = s"""
    select 
    A.*,
    incdata_id,
    'cguid' as join_strategy
    from sessions A
    inner join cguid       B
    on A.cguid  =  B.ident_attr_value_txt
    """
    spark.sql(cguid_join_query).createOrReplaceTempView("cguid_join")
    //==========================================================================

    
    val join_sql = """
    select
    coalesce(A.guid,B.guid, C.guid) as guid,
    coalesce(A.cguid,B.cguid, C.cguid) as cguid,
    coalesce(A.session_skey,B.session_skey, C.session_skey) as session_skey,
    coalesce(A.site_id,B.site_id, C.site_id) as site_id,
    coalesce(A.session_start_dt,B.session_start_dt, C.session_start_dt) as session_start_dt,
    coalesce(A.user_id,B.user_id, C.user_id) as user_id,
    coalesce(A.session_traffic_source_id,B.session_traffic_source_id, C.session_traffic_source_id) as session_traffic_source_id,
    coalesce(A.num_fashion,B.num_fashion, C.num_fashion) as num_fashion,
    coalesce(A.num_home_garden,B.num_home_garden, C.num_home_garden) as num_home_garden,
    coalesce(A.num_electronics,B.num_electronics, C.num_electronics) as num_electronics,
    coalesce(A.num_collectibles,B.num_collectibles, C.num_collectibles) as num_collectibles,
    coalesce(A.num_parts_acc,B.num_parts_acc, C.num_parts_acc) as num_parts_acc,
    coalesce(A.num_vehicles,B.num_vehicles, C.num_vehicles) as num_vehicles,
    coalesce(A.num_lifestyle,B.num_lifestyle, C.num_lifestyle) as num_lifestyle,
    coalesce(A.num_bu_industrial,B.num_bu_industrial, C.num_bu_industrial) as num_bu_industrial,
    coalesce(A.num_media,B.num_media, C.num_media) as num_media,
    coalesce(A.num_real_estate,B.num_real_estate, C.num_real_estate) as num_real_estate,
    coalesce(A.num_unknown,B.num_unknown, C.num_unknown) as num_unknown,
    coalesce(A.num_clothes_shoes_acc,B.num_clothes_shoes_acc, C.num_clothes_shoes_acc) as num_clothes_shoes_acc,
    coalesce(A.num_home_furniture,B.num_home_furniture, C.num_home_furniture) as num_home_furniture,
    coalesce(A.num_cars_motors_vehic,B.num_cars_motors_vehic, C.num_cars_motors_vehic) as num_cars_motors_vehic,
    coalesce(A.num_cars_p_and_acc,B.num_cars_p_and_acc, C.num_cars_p_and_acc) as num_cars_p_and_acc,
    coalesce(A.num_sporting_goods,B.num_sporting_goods, C.num_sporting_goods) as num_sporting_goods,
    coalesce(A.num_toys_games,B.num_toys_games, C.num_toys_games) as num_toys_games,
    coalesce(A.num_health_beauty,B.num_health_beauty, C.num_health_beauty) as num_health_beauty,
    coalesce(A.num_mob_phones,B.num_mob_phones, C.num_mob_phones) as num_mob_phones,
    coalesce(A.num_business_ind,B.num_business_ind, C.num_business_ind) as num_business_ind,
    coalesce(A.num_baby,B.num_baby, C.num_baby) as num_baby,
    coalesce(A.num_jewellry_watches,B.num_jewellry_watches, C.num_jewellry_watches) as num_jewellry_watches,
    coalesce(A.num_computers,B.num_computers, C.num_computers) as num_computers,
    coalesce(A.num_modellbau,B.num_modellbau, C.num_modellbau) as num_modellbau,
    coalesce(A.num_antiquities,B.num_antiquities, C.num_antiquities) as num_antiquities,
    coalesce(A.num_sound_vision,B.num_sound_vision, C.num_sound_vision) as num_sound_vision,
    coalesce(A.num_video_games_consoles,B.num_video_games_consoles, C.num_video_games_consoles) as num_video_games_consoles,
    coalesce(A.incdata_id,B.incdata_id) as incdata_id,
    coalesce(A.join_strategy, B.join_strategy, 'no_join') as join_strategy,
    coalesce(A.session_start_dt, B.session_start_dt, C.session_start_dt) as dt

    from               uid_join A
    full outer join    cguid_join b
    on                 A.guid         = B.guid
    and                A.session_skey = B.session_skey
    and                A.site_id      = B.site_id

    full outer join   sessions C
    on                 A.guid         = C.guid
    and                A.session_skey = C.session_skey
    and                A.site_id      = C.site_id
    """
    
    
    val rowNumb = (row_number()
		   .over(Window.partitionBy(col("guid"), col("session_skey"), col("site_id"),  col("incdata_id"))
			 .orderBy(col("user_id").asc_nulls_last)
		       )
		 )
      
    val sess_with_items = (spark
			   .sql(join_sql)
			   .select($"*", rowNumb as "rnum")
			     .filter($"rnum"===1)
			 )      

      (sess_with_items
       .write
       .partitionBy("dt")
       .mode(SaveMode.Append)
       .save("hdfs://apollo-phx-nn-ha/user/hive/warehouse/mktng.db/sess_with_item_counts_with_xid")
       )   
  }
}
