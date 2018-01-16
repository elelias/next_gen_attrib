import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object ChannelPairs {

  def main(args: Array[String]) {

    val spark = SparkSession.builder.appName("channel_pairs").master("yarn").getOrCreate()
    import spark.implicits._

    /*
     * You can copy and paste from here to the remote spark-shell
     */

    import org.apache.spark.sql.expressions.Window
    val transactions_sample = spark.read.load("hdfs://apollo-phx-nn-ha/user/hive/warehouse/attrib.db/me_txns_correlation_subsample")

    val marketing_info = transactions_sample.select(
      $"transaction_id",
      $"item_id",
      $"event_ts",
      when($"event_type_id".isin(1, 5, 7, 8), "click").when($"event_type_id" === 6, "open").otherwise("undefined").as("event_type"),
      coalesce($"channel_name", lit("unassigned")).as("channel_name")
    ).where($"transaction_id".isNotNull and $"item_id".isNotNull and $"event_type_id" =!= 4)

    val window = Window.partitionBy("transaction_id", "item_id").orderBy("event_ts")
    val pairs =
      marketing_info.
        withColumn("previous_event_type", lag("event_type", 1, null).over(window)).
        withColumn("previous_channel_name", lag("channel_name", 1, null).over(window)).
        withColumn("step", rank().over(window))

    val channel_pairs_path = "hdfs://apollo-phx-nn-ha/user/hive/warehouse/attrib.db/channel_pairs"

    pairs.
      write.
      mode("overwrite").
      parquet(channel_pairs_path)

    val weighted_edges =
      spark.read.option("sep", "\t").parquet(channel_pairs_path).
        groupBy("channel_name", "event_type", "previous_channel_name", "previous_event_type", "step").
        count().withColumnRenamed("count", "weight")

    weighted_edges.write.mode("overwrite").parquet("hdfs://apollo-phx-nn-ha/user/hive/warehouse/attrib.db/weighted_edges")

    /*
     * Stop here copying and pasting to the remote spark-shell to prevent closing your spark session
     */

    spark.stop()
  }

}