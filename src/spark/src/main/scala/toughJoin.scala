
/* SimpleApp.scala */
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession


object ToughJoinApp {

 def main(args: Array[String]) {
    //val logFile = "YOUR_SPARK_HOME/README.md"  Should be some file on your system

    val spark = SparkSession.builder.appName("tough_join").master("yarn").getOrCreate()

   import spark.implicits._
   import spark.sql

   val elig_list_df = spark.read.load("hdfs://apollo-phx-nn-ha/user/hive/warehouse/erondb.db/eron_eligible_listings_parquet").withColumnRenamed("site_id","elig_site_id").withColumnRenamed("leaf_categ_id","elig_leaf_categ_id")
   val flrw_catg_df = spark.read.load("hdfs://apollo-phx-nn-ha/user/hive/warehouse/erondb.db/eron_flwr_with_relevant_categories_us_parquet")
   println(elig_list_df.columns)
   elig_list_df.createOrReplaceTempView("elig")
   flrw_catg_df.createOrReplaceTempView("catg")
   val join_df = sql("select * from elig A join catg B ON A.slr_id = B.flwd_user_id and A.elig_leaf_categ_id = B.leaf_categ_id")
   //#val join_df = elig_list_df.join(flrw_catg_df, (elig_list_df.slr_id == flrw_catg_df.flwd_user_id) & (elig_list_df.elig_leaf_categ_id == flrw_catg_df.leaf_categ_id ))
   join_df.write.save("hdfs://apollo-phx-nn-ha/user/hive/warehouse/erondb.db/tough_join_in_spark_test_scala.parquet")

   spark.stop()
  }
}
