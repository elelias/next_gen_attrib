/* pre_bbowa_channel_type_analysis.scala */
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import scala.collection.mutable
import scala.collection._

object PreBBOWAChannelEventAnalysisApp {

 def main(args: Array[String]) {

    val spark = SparkSession.builder.appName("pre_bbowa_channel_event_analysis").master("yarn").getOrCreate()
    import spark.implicits._
    import spark.sql

    val txns_df = spark.read.load("hdfs://apollo-phx-nn-ha/user/hive/warehouse/attrib.db/me_txns_correlation_subsample")

    txns_df.createOrReplaceTempView("sample")
    
    /*
        Select relevant marketing-related info from the table.
    */
    val marketing_info = sql("""select
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
    from sample
    where bbowa_sec_diff > 0""")


    marketing_info.createOrReplaceTempView("marketing_info")

    /*
        Create an encoder table.

        The channels will be encoded in a 0-1 vector where 1 on position N means that channel#event#device corresponding
        to N is present in the path.

        The encoding with decreasing frequency of the channel#event#device combinations, so that we can restrict to more
        frequently occuring combinations by truncating the indicator vector.
    */
    val channel_name_encoder = sql("""
        select
          channel_event_device,
          row_number() over(order by count(*) desc) - 1 as n
        from
          marketing_info
        group by channel_event_device""").cache()

    channel_name_encoder.write.option("sep","\t").mode("overwrite").csv("hdfs://apollo-phx-nn-ha/user/hive/warehouse/attrib.db/pre_bbowa_channel_event_analysis_encoder")

    /*
        INPUT: Sequence of channel#event#device strings
        OUTPUT: 0/1 coding vector according to the encoder
    */
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

    /*
        Group table according to the unique transaction key along with unique transaction info, collect all related marketing
        events as list and encode the list to 0/1 vector.
    */
    val encoded_channels = marketing_info
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
      
   encoded_channels.write.option("sep","\t").mode("overwrite").csv("hdfs://apollo-phx-nn-ha/user/hive/warehouse/attrib.db/pre_bbowa_channel_event_analysis")

   spark.stop()
  }

}