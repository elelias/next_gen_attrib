import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object reg_sessions_with_xid {

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


   val start_dt   = args(0)
   val end_dt     = args(1)


   //===========================
   // XID 
   //===========================
   val inc_master_df   = spark.read.load("hdfs://apollo-phx-nn-ha/user/hive/warehouse/mktng.db/inc_master_low_skew_parquet")
   val cguid_to_xid_df = spark.read.load("hdfs://apollo-phx-nn-ha/user/hive/warehouse/mktng.db/incdata_cguid_to_xid_low_skew_parquet")
   val did_to_xid_df   = spark.read.load("hdfs://apollo-phx-nn-ha/user/hive/warehouse/mktng.db/incdata_did_to_xid_low_skew_parquet")

   inc_master_df.createOrReplaceTempView("inc_master")
   cguid_to_xid_df.createOrReplaceTempView("cguid")
   did_to_xid_df.createTempView("did")



   //============================
   //UNREGISTERED SESSIONS
   //============================
   spark.read.load("hdfs://apollo-phx-nn-ha/user/hive/warehouse/mktng.db/signedin_users").createOrReplaceTempView("unreg_sess")

   val unreg_df = spark.sql(s"select * from unreg_sess where reg_dt between '$start_dt' and '$end_dt'")
   unreg_df.cache()
   unreg_df.createOrReplaceTempView("unreg_sess_indate")

   val non_null_did_query = """
   select * from unreg_sess_indate
   where LENGTH(device_id)>=35 
   and device_id not like '%00000000-0000-0000-0000-000000000000%'
   """
   spark.sql(non_null_did_query).createOrReplaceTempView("unreg_non_null_did")






   //============================


   
   val all_join_sql = """
   select
   coalesce(A.guid,B.guid, C.guid, D.guid) as guid,
   coalesce(A.cguid,B.cguid, C.cguid, D.cguid) as cguid,
   coalesce(A.parent_uid,B.parent_uid, C.parent_uid, D.parent_uid) as parent_uid,
   coalesce(A.session_skey,B.session_skey, C.session_skey, D.session_skey) as session_skey,
   coalesce(A.site_id,B.site_id, C.site_id, D.site_id) as site_id,
   coalesce(A.cobrand,B.cobrand, C.cobrand, D.cobrand) as cobrand,
   coalesce(A.session_start_dt,B.session_start_dt, C.session_start_dt, D.session_start_dt) as session_start_dt,
   coalesce(A.start_timestamp,B.start_timestamp, C.start_timestamp, D.start_timestamp) as start_timestamp,
   coalesce(A.end_timestamp,B.end_timestamp, C.end_timestamp, D.end_timestamp) as end_timestamp,
   coalesce(A.primary_app_id,B.primary_app_id, C.primary_app_id, D.primary_app_id) as primary_app_id,
   coalesce(A.session_traffic_source_id,B.session_traffic_source_id, C.session_traffic_source_id, D.session_traffic_source_id) as session_traffic_source_id,
   coalesce(A.cust_traffic_source_level1,B.cust_traffic_source_level1, C.cust_traffic_source_level1, D.cust_traffic_source_level1) as cust_traffic_source_level1,
   coalesce(A.cust_traffic_source_level2,B.cust_traffic_source_level2, C.cust_traffic_source_level2, D.cust_traffic_source_level2) as cust_traffic_source_level2,
   coalesce(A.traffic_source_level3,B.traffic_source_level3, C.traffic_source_level3, D.traffic_source_level3) as traffic_source_level3,
   coalesce(A.rotation_id,B.rotation_id, C.rotation_id, D.rotation_id) as rotation_id,
   coalesce(A.rvr_id,B.rvr_id, C.rvr_id, D.rvr_id) as rvr_id,
   coalesce(A.idfa,B.idfa, C.idfa, D.idfa) as idfa,
   coalesce(A.gadid,B.gadid, C.gadid, D.gadid) as gadid,
   coalesce(A.gdid,B.gdid, C.gdid, D.gdid) as gdid,
   coalesce(A.device_id,B.device_id, C.device_id, D.device_id) as device_id,
   coalesce(A.device_type,B.device_type, C.device_type, D.device_type) as device_type,
   coalesce(A.device_type_level1,B.device_type_level1, C.device_type_level1, D.device_type_level1) as device_type_level1,
   coalesce(A.device_type_level2,B.device_type_level2, C.device_type_level2, D.device_type_level2) as device_type_level2,
   coalesce(A.experience_level1,B.experience_level1, C.experience_level1, D.experience_level1) as experience_level1,
   coalesce(A.experience_level2,B.experience_level2, C.experience_level2, D.experience_level2) as experience_level2,
   coalesce(A.vi_cnt,B.vi_cnt, C.vi_cnt, D.vi_cnt)         as vi_cnt,
   coalesce(A.incdata_id,B.incdata_id, C.incdata_id  )         as incdata_id,
   coalesce(A.join_strategy, B.join_strategy, C.join_strategy, 'nojoin') as join_strategy,
   coalesce(A.reg_dt, B.reg_dt, C.reg_dt, D.reg_dt)        as dt

   from              uid_join A
   full outer join   did_join       B
   on              A.guid         = B.guid
   and             A.session_skey = B.session_skey


   full outer join    cguid_join    C
   on              A.guid         = C.guid
   and             A.session_skey = C.session_skey

   full outer join unreg_sess_indate  D
   on              A.guid           = D.guid
   and             A.session_skey   = D.session_skey
   """
   




   
   val runreg_query = s"""select * from unreg_sess 
   where reg_dt between '${start_dt}' and '${end_dt}' 
   """

   val uid_join_query = s"""
   select 
   A.*,
   incdata_id,
   'uid' as join_strategy
   from unreg_sess_indate              A
   inner join inc_master               B
   on A.parent_uid = B.incdata_srcid
   """  


  

   val cguid_join_query = s"""
   select 
   A.*,
   incdata_id,
   'cguid' as join_strategy
    from unreg_sess_indate         A
    inner join cguid               B
    on A.cguid = B.ident_attr_value_txt 

   """  


   val did_join_query = s"""
   select 
   A.*,
   incdata_id,
   'did' as join_strategy
   from       unreg_non_null_did   A
   inner join  did                  B
   on A.device_id = B.ident_attr_value_txt     
   """  

   //NON-NULL UID
   spark.sql(uid_join_query).createOrReplaceTempView("uid_join")
   //
   //NON-NULL CGUID
   spark.sql(cguid_join_query).createOrReplaceTempView("cguid_join")
   //
   //NON-NULL DID
   spark.sql(did_join_query).createOrReplaceTempView("did_join")

   //QUALIFIER
   val rNumb = (row_number()
            .over(Window.partitionBy(col("guid"), col("session_skey"))
		    .orderBy(col("device_type_level1"), col("device_type_level2"),col("experience_level1"),col("experience_level2"))
		  )
            )


   val path="hdfs://apollo-phx-nn-ha/user/hive/warehouse/mktng.db/sessions_signedin_xid_parquet"     

   val full_join_df = (spark
		       .sql(all_join_sql)
		       .select($"*", rNumb as "rnum")
		       ).filter($"rnum"===1)
      
   (full_join_df
    .write
    .partitionBy("dt")
    .mode(SaveMode.Append)
    .save(path)
  )
 }
}
