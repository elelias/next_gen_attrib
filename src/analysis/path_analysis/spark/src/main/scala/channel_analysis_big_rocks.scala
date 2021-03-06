import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object ChannelPairs {

  def main(args: Array[String]) {

    val spark = SparkSession.builder.appName("channel_pairs").master("yarn").getOrCreate()
    import spark.implicits._

    /*
     * You can copy and paste from here to the remote spark-shell
     */

    println(s"process started at ${java.time.Instant.now}")

    spark.sparkContext.setLogLevel("FATAL")

    import org.apache.spark.sql.expressions.{Window, WindowSpec}
    import org.apache.spark.sql.Column

    val txn_path = "hdfs://apollo-phx-nn-ha/user/hive/warehouse/attrib.db/me_txns_correlation_20171101"
    val transactions_sample = spark.read.load(txn_path)
    //
    //
    transactions_sample.createOrReplaceTempView("txns")
    //
    //
    val valid_transaction: Column = $"transaction_id".isNotNull and $"item_id".isNotNull
    val click: Column = $"event_type_id".isin(1, 5, 7, 8)
    val open: Column = $"event_type_id" === 6
    val impression: Column = $"event_type_id" === 4
    val device_pc: Column = $"bbowa_device_type"==="PC"
    val device_null: Column = $"bbowa_device_type".isNull
    val device_phone: Column = $"bbowa_device_type" === "Phone"
    val device_Tablet: Column = $"bbowa_device_type" === "Tablet"
    val device_pcTouch: Column = $"bbowa_device_type" === "PC: Touch"
    val channel_epn: Column = $"channel_name" === "epn"
    val channel_display: Column = $"channel_name" === "Display"
    val no_organic: Column = $"current.channel_name" !== "organic" and $"current.channel_name" !== "site_emails"


    val partition_by_checkout_order_by_timestamp: WindowSpec =
      Window.partitionBy($"transaction_id", $"item_id").orderBy($"event_ts")

    val partition_by_checkout_order_by_reversed_timestamp: WindowSpec =
      Window.partitionBy($"transaction_id", $"item_id").orderBy($"event_ts".desc)

    val display_brock = (spark
      .read
      .option("delimiter","\t").csv("hdfs://apollo-phx-nn-ha/user/hive/warehouse/mktng.db/display_big_rocks")
      .selectExpr("_c0 as rotation_id", "_c1 as big_rock")
      .withColumn("rotation_id", $"rotation_id".cast("long"))
      )
    

    display_brock.createOrReplaceTempView("brock")


    
    val query = """    
    select 
    A.*,
    B.big_rock
    from      txns A
    left join brock     B
    on A.rotation_id = B.rotation_id    
    """
    val mktng_with_brock_df = sql(query)





    mktng_with_brock_df.write.mode("overwrite").parquet("hdfs://apollo-phx-nn-ha/user/hive/warehouse/attrib.db/mktng_with_brock_df")



    



    val marketing_info = mktng_with_brock_df.select(
      $"transaction_id",
      $"item_id",
      struct(
	coalesce($"fm_buyer_type_desc", lit("unknown")).as("fm_buyer_type_desc") ,
	coalesce($"is_first_purchase", lit(0)).as("is_first_purchase"),	
	when(device_pc,"PC")
	.when(device_null,"unknown")
	.when(device_phone, "Mobile")
	.when(device_Tablet, "Tablet")
	.when(device_pcTouch,"PC")
	.otherwise("others")
	.as("bbowa_device")
      ).as("transaction"),
	
      $"event_ts",
      $"event_id",

      struct(
        when(click, "click").when(open, "open").otherwise("undefined").as("event_type"),
        coalesce($"device_type_level1", lit("unassigned")).as("event_device"),
	coalesce($"channel_name", lit("unassigned")).as("channel_name"),
	when(channel_epn, $"flex_column_1").when(channel_display, $"big_rock").otherwise("").as("sub_channel")
        //coalesce($"channel_name", lit("unassigned")).as("channel_name")	
      ).as("current")     
    ).where(valid_transaction and not(impression))

    









    marketing_info.cache()
    marketing_info.write.mode("overwrite").parquet("hdfs://apollo-phx-nn-ha/user/hive/warehouse/attrib.db/marketing_info")

    
    val no_organic_df = marketing_info.where(no_organic)

    val ordered_info = marketing_info.
    groupBy("transaction_id", "item_id", "transaction").
    agg(countDistinct($"event_id") as "path_length",
	 sort_array(collect_list(struct("event_ts", "event_id", "current"))) as "mktng_events"
       ).
    filter($"path_length" <= 100)


    ordered_info.cache()
    
    
    val chrono_pairs = marketing_info.
    withColumn("next", lead("current", 1, null).over(partition_by_checkout_order_by_timestamp)).
    withColumn("forward_step", rank().over(partition_by_checkout_order_by_timestamp)).
    withColumn("previous", lag("current", 1, null).over(partition_by_checkout_order_by_reversed_timestamp)).
    withColumn("backward_step", rank().over(partition_by_checkout_order_by_reversed_timestamp))
    //
    //




    val chrono_events = chrono_pairs.
    groupBy("transaction_id", "item_id", "transaction").
    agg(countDistinct($"event_id") as "path_length",
	sort_array(collect_list(struct("event_ts", "event_id", "current.channel_name","current.sub_channel","previous.channel_name","previous.sub_channel", "backward_step","next.channel_name","next.sub_channel", "forward_step"))) as "mktng_events"
      )
    
    





    

    val weighted_edges =
      chrono_pairs.
        groupBy("transaction", "current", "next", "step").
        count().withColumnRenamed("count", "weight")

    weighted_edges.write.mode("overwrite").parquet("hdfs://apollo-phx-nn-ha/user/hive/warehouse/attrib.db/weighted_edges_epn_display_chrono")

    println(s"process ended at ${java.time.Instant.now}")

    /*
     * Stop here copying and pasting to the remote spark-shell to prevent closing your spark session
     */

    spark.stop()
  }

}
