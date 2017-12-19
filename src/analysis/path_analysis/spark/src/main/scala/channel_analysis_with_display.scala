/* SimpleApp.scala */
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import scala.collection.mutable
import scala.collection._

object ChannelAnalysisDisplayApp {

 def main(args: Array[String]) {

    val spark = SparkSession.builder.appName("channel_analysis_display").master("yarn").getOrCreate()
    import spark.implicits._
    import spark.sql

    val txns_df = spark.read.load("hdfs://apollo-phx-nn-ha/user/hive/warehouse/attrib.db/me_txns_correlation_subsample")

    txns_df.createOrReplaceTempView("sample")

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
    coalesce(channel_name, "Unassigned") as channel_name
    from sample""")


    temp.createOrReplaceTempView("temp")

    val channel_name_encoder = sql("""
        select
          channel_name,
          cast(power(2.0, row_number() over(order by count(*) desc) - 1) as int) as n
        from
          temp
        group by channel_name""").cache()

    channel_name_encoder.write.option("sep","\t").mode("overwrite").csv("hdfs://apollo-phx-nn-ha/user/hive/warehouse/attrib.db/channel_analysis_encoder")


    def encode_channels(channels: Seq[String], channel_name_encoder: Map[String, Int]): Int = {
    	if (channels == null || channel_name_encoder == null) 0
	else channels.map(c => channel_name_encoder.getOrElse(c, 0)).sum
    }

    val channel_name_map = channel_name_encoder.collect()
                                        .map(r => r.getString(0) -> r.getInt(1))
					.toMap	

    def encode_channels2(channels: Seq[String]): Int = encode_channels(channels, channel_name_map)

    val encode_channels_udf = udf(encode_channels2 _)

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
      .agg(encode_channels_udf(collect_list("channel_name")) as "channel_names")
      
   //u.repartition($"mktng_dt").write.partitionBy("mktng_dt").option("sep","\t").mode("overwrite").csv("hdfs://apollo-phx-nn-ha/user/hive/warehouse/attrib.db/channel_analysis")

   u.write.option("sep","\t").mode("overwrite").csv("hdfs://apollo-phx-nn-ha/user/hive/warehouse/attrib.db/channel_analysis")

   spark.stop()
  }

}