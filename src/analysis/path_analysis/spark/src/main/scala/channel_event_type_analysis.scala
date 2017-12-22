/* SimpleApp.scala */
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import scala.collection.mutable
import scala.collection._

object ChannelEventAnalysisApp {

 def main(args: Array[String]) {

    val spark = SparkSession.builder.appName("channel_event_analysis").master("yarn").getOrCreate()
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
    concat(
	coalesce(channel_name, "unassigned"),
	'#',
    	case when event_type_id in (1, 5, 7, 8) then "click"
    	     when event_type_id = 4 then "impression"
	     when event_type_id = 6 then "open"
	     else "undefined" 
             end,
	 '#',
    	 case when event_type_id = 4 and channel_id = 1 and device_type_level1 is null then "PC"
              else coalesce(device_type_level1,"unknown")
              end) as channel_event_device
    from sample""")


    temp.createOrReplaceTempView("temp")

    val channel_name_encoder = sql("""
        select
          channel_event_device,
          row_number() over(order by count(*) desc) - 1 as n
        from
          temp
        group by channel_event_device""").cache()

    channel_name_encoder.write.option("sep","\t").mode("overwrite").csv("hdfs://apollo-phx-nn-ha/user/hive/warehouse/attrib.db/channel_event_analysis_encoder")


    def encode_channels(channels: Seq[String], channel_name_encoder: Map[String, Int]):  String = {
    	if (channel_name_encoder == null) ""
	else {
	    val bitSet = Array.fill[Byte](channel_name_encoder.size)(0)
	    channels.foreach(channel => bitSet(channel_name_encoder.getOrElse(channel, 0)) = 1)
	    bitSet.mkString
	}
    }

    val channel_name_map = channel_name_encoder.collect()
                                        .map(r => r.getString(0) -> r.getInt(1))
					.toMap	

    def encode_channels2(channels: Seq[String]): String = encode_channels(channels, channel_name_map)

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
      .agg(encode_channels_udf(collect_list("channel_event_device")) as "channel_event_device")
      
   //u.repartition($"mktng_dt").write.partitionBy("mktng_dt").option("sep","\t").mode("overwrite").csv("hdfs://apollo-phx-nn-ha/user/hive/warehouse/attrib.db/channel_event_analysis")

   u.write.option("sep","\t").mode("overwrite").csv("hdfs://apollo-phx-nn-ha/user/hive/warehouse/attrib.db/channel_event_analysis")

   spark.stop()
  }

}