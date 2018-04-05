import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window


/* ***************************
// Tables used :
site categories per session: "hdfs://apollo-phx-nn-ha/user/hive/warehouse/mktng.db/view_items_external_parquet_ana"
repartitioned using guid, session_skey : "hdfs://apollo-phx-nn-ha/user/hive/warehouse/mktng.db/view_items_repartitioned_external_parquet_ana"
specificity metric: "hdfs://apollo-phx-nn-ha/user/hive/warehouse/mktng.db/specificity_parquet_ana"
checkout_metric_items: "hdfs://apollo-phx-nn-ha/user/hive/warehouse/mktng.db/view_checkout_metric_items_external_parquet_ana"
repartitioned using guid, session_skey :
  *************************** */


object session_specificity_metric {

  def main(args: Array[String]) {

    val spark = SparkSession.builder
      .master("yarn")
      .appName("specificity")
      .getOrCreate()

 
    import spark.implicits._
    import spark.sql
    import org.apache.spark.sql.SaveMode
 


    spark
      .sparkContext
      .setLogLevel("ERROR")


    println("Reading parquet file ...")
    val view_items_all_df = spark
      .read
      .load("hdfs://apollo-phx-nn-ha/user/hive/warehouse/mktng.db/view_items_cats_parquet_external_ana")
    //
    //
    val sess_start_dt    = args(0)
    val sess_end_dt      = args(1)
    println(s"$sess_start_dt and $sess_end_dt")
    //
    view_items_all_df.createOrReplaceTempView("view_items_all")
    //
    //
    val view_items_df = spark.sql(s"select * from view_items_all where dt between '$sess_start_dt' and '$sess_end_dt'")
    //
    //




    val df_numCats= view_items_df
      .groupBy($"guid", $"session_skey", $"site_id", $"dt")
      .agg(countDistinct($"meta_categ_name") as "num_meta", 
	   countDistinct($"categ_lvl2_id") as "num_lv2", 
	   countDistinct($"categ_lvl3_id") as "num_lv3", 
	   countDistinct($"categ_lvl4_id") as "num_lv4")

    // Generate the bins - 5 bins in this case
    val dfBucketized = df_numCats
      .withColumn("meta_bucket", when($"num_meta" === 1, 1)
        .otherwise(when($"num_meta" === 2, 2)
          .otherwise(when($"num_meta" >=3 && $"num_meta" <= 4, 3)
            .otherwise(when($"num_meta" >=5 && $"num_meta" <= 7, 4)
              .otherwise(5)))))
      .withColumn("lv2_bucket", when($"num_lv2" === 1, 1)
        .otherwise(when($"num_lv2" >= 2 && $"num_lv2" <= 3, 2)
          .otherwise(when($"num_lv2" >=4 && $"num_lv2" <= 6, 3)
            .otherwise(when($"num_lv2" >=7 && $"num_lv2" <= 12, 4)
              .otherwise(5)))))
      .withColumn("lv3_bucket", when($"num_lv3" >=1 && $"num_lv3" <= 2, 1)
        .otherwise(when($"num_lv3" >= 3 && $"num_lv3" <= 5, 2)
          .otherwise(when($"num_lv3" >=6 && $"num_lv3" <= 8, 3)
            .otherwise(when($"num_lv3" >=9 && $"num_lv3" <= 15, 4)
              .otherwise(5)))))
      .withColumn("lv4_bucket", when($"num_lv4" >=1 && $"num_lv4" <= 2, 1)
        .otherwise(when($"num_lv4" >= 3 && $"num_lv4" <= 5, 2)
          .otherwise(when($"num_lv4" >=6 && $"num_lv4" <= 8, 3)
            .otherwise(when($"num_lv4" >=9 && $"num_lv4" <= 15, 4)
              .otherwise(5)))))

    println("Estimating specificity value ...")
    // Determine specificty
    val specUDF = udf(specificity _)
    val dfSpec = dfBucketized
      .withColumn("specificity", specUDF(dfBucketized("meta_bucket"), dfBucketized("lv2_bucket"), dfBucketized("lv3_bucket"), dfBucketized("lv4_bucket"), lit(5)))
      .select($"guid", $"session_skey", $"dt", $"site_id", $"num_meta", $"num_lv2", $"num_lv3", $"num_lv4", $"specificity")

    val dfSpecBinned = dfSpec
      .withColumn("specificity_binned", floor(dfSpec("specificity") * 10) - 1 )


    println("Writing the specificity value ...")
    // Store the dataframe
    dfSpecBinned
      .write
      .partitionBy("dt")
      .mode(SaveMode.Append)
      .parquet("hdfs://apollo-phx-nn-ha/user/hive/warehouse/mktng.db/specificity_parquet_eron")
  }

  // Normalized specificity
  def specificity(meta: Int, lv2: Int, lv3: Int, lv4: Int, nB:Int): Double ={
    val w0 = Math.pow(nB, 3)
    val w1 = Math.pow(nB, 2)
    val w2 = nB
    val w3 = 1

    (w0*meta + w1*lv2 + w2*lv3 + w3*lv4)/(w0*nB + w1*nB + w2*nB + w3*nB)
  }
}
