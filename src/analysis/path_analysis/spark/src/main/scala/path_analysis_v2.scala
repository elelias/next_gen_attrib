/* SimpleApp.scala */
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import scala.collection.mutable
import scala.collection._

object PathAnalysis2App {

 def main(args: Array[String]) {

    val spark = SparkSession.builder.appName("path_analysis_v2").master("yarn").getOrCreate()
    import spark.implicits._
    import spark.sql

    val txns_df = spark.read.option("basePath", "hdfs://apollo-phx-nn-ha/user/hive/warehouse/mktng.db/marketing_events_txns_correlation").load("hdfs://apollo-phx-nn-ha/user/hive/warehouse/mktng.db/me_txns_correlation/*/ck_dt=2017-11-01")
    val more_days_txns = txns_df.filter("mktng_dt between '2017-10-13' and '2017-11-01'")
    more_days_txns.createOrReplaceTempView("more_days")

    val sampled_df = more_days_txns.sample(false, 1)
    sampled_df.createOrReplaceTempView("sample")



    val temp = sql("""select
    distinct
    coalesce(transaction_id, -1) as transaction_id,
    coalesce(item_id, -1) as item_id,
    auct_end_dt,
    coalesce(item_site_id, -1) as item_site_id,
    coalesce(auct_type_code, -1) as auct_type_code,
    coalesce(leaf_categ_id, -1) as leaf_categ_id,
    coalesce(seller_id, -1) as seller_id,
    coalesce(seller_country_id, -1) as seller_country_id,
    coalesce(buyer_id, -1) as buyer_id,
    coalesce(buyer_country_id, -1) as buyer_country_id,
    coalesce(fm_buyer_type_cd, -1) as fm_buyer_type_cd,
    coalesce(fm_buyer_type_desc, -1) as fm_buyer_type_desc,
    coalesce(is_first_purchase, -1) as is_first_purchase,
    created_dt,
    coalesce(gmb_usd, 0) as gmb_usd,
    coalesce(event_id, "") as event_id,
    event_ts,
    coalesce(channel_name, "Unassigned") as channel_name
    from sample""")

    def coocurences(events: Seq[Row]): Seq[(String, String, Int)] = {
       events.map(x => (x.getString(0), x.getString(1))).combinations(2)
        .toList
        .map{case Seq(s1, s2) => (s1,s2)}
        .groupBy(v => if (v._1._2 <= v._2._2) (v._1._2, v._2._2) else (v._2._2, v._1._2))
        .map(x => (x._1._1, x._1._2, x._2.length))
        .toSeq
    }
    
    val coocurences_udf = udf(coocurences _)

    def coocurences_aggregation(events: Seq[Seq[Row]]): String = {
      events.flatMap(l => l)
        .map(x => ((x.getString(0), x.getString(1)), x.getInt(2)))
        .groupBy(v => v._1)
        .mapValues{l => l.map(v => v._2).sum}
        .mkString(";")
    }

    val coocurences_aggregation_udf = udf(coocurences_aggregation _)


    val u = temp
      .groupBy( 
      "transaction_id",
      "item_id",
      "auct_end_dt",
      "item_site_id",
      "auct_type_code",
      "leaf_categ_id",
      "seller_id",
      "seller_country_id",
      "buyer_id",
      "buyer_country_id",
      "fm_buyer_type_cd",
      "fm_buyer_type_desc",
      "is_first_purchase",
      "created_dt",
      "gmb_usd")
      .agg(countDistinct($"event_id") as "path_length",
           collect_list(struct("event_id", "channel_name")) as "mktng_events")
      .filter($"path_length" <= 60)
      .withColumn("coocurences", coocurences_udf($"mktng_events"))
      .select($"path_length", 
              $"coocurences")
      .groupBy("path_length")
      .agg(coocurences_aggregation_udf(collect_list($"coocurences")) as "coocurence_pairs")
      
    u.write.option("sep","\t").mode("overwrite").csv("hdfs://apollo-phx-nn-ha/user/hive/warehouse/mktng.db/path_analysis_v2/coocurence_pairs_by_length/ck_dt=2017-11-01")

    spark.stop()
  }

}