import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object ChannelPairs {

  def main(args: Array[String]) {

    val spark = SparkSession.builder.appName("channel_pairs").master("yarn").getOrCreate()
    import spark.sql

    val txns_df = spark.read.load("hdfs://apollo-phx-nn-ha/user/hive/warehouse/attrib.db/me_txns_correlation_subsample")

    txns_df.createOrReplaceTempView("sample")

    // Select relevant marketing-related info from the table.
    val marketing_info = sql(
      """select
    distinct
    coalesce(transaction_id, -1) as transaction_id,
    coalesce(item_id, -1) as item_id,
    event_ts,
	  coalesce(channel_name, "unassigned") as channel_name
    from sample
    where event_type_id <> 4""")

    val window = Window.partitionBy("transaction_id", "item_id").orderBy("event_ts")
    val pairs = marketing_info.withColumn("previous_channel_name", lag("channel_name", 1, null).over(window)).withColumn("rank", rank().over(window))

    val channel_pairs_path = "hdfs://apollo-phx-nn-ha/user/hive/warehouse/attrib.db/channel_pairs"

    // Write the channel pairs (unless they are already there)
    pairs
      .write
      .option("sep", "\t")
      .mode("ignore")
      .csv(channel_pairs_path)

    val weighted_edges =
      spark.read.option("sep", "\t").csv(channel_pairs_path).
        toDF("transaction_id", "item_id", "event_ts", "channel_name", "previous_channel_name", "step").
        groupBy("channel_name", "previous_channel_name", "step").
        count().withColumnRenamed("count", "weight")

    weighted_edges.write.options(Map("sep" -> "\t", "header" -> "true")).mode("ignore")

    spark.stop()
  }

}