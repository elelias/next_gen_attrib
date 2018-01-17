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

    import org.apache.spark.sql.expressions.Window
    import org.apache.spark.sql.Column

    val transactions_sample = spark.read.load("hdfs://apollo-phx-nn-ha/user/hive/warehouse/attrib.db/me_txns_correlation_subsample")

    val valid: Column = $"transaction_id".isNotNull and $"item_id".isNotNull
    val click: Column = $"event_type_id".isin(1, 5, 7, 8)
    val open: Column = $"event_type_id" === 6
    val impression: Column = $"event_type_id" === 4

    val marketing_info = transactions_sample.select(
      $"transaction_id",
      $"item_id",
      $"event_ts",
      when(click, "click").when(open, "open").otherwise("undefined").as("event_type"),
      coalesce($"device_type_level1", lit("unassigned")).as("event_device"),
      coalesce($"channel_name", lit("unassigned")).as("channel_name")
    ).where(valid and not(impression))

    val window = Window.partitionBy("transaction_id", "item_id").orderBy("event_ts")
    val pairs = marketing_info.
      withColumn("previous_event_type", lag("event_type", 1, null).over(window)).
      withColumn("previous_event_device", lag("event_device", 1, null).over(window)).
      withColumn("previous_channel_name", lag("channel_name", 1, null).over(window)).
      withColumn("step", rank().over(window))

    val channel_pairs_path = "hdfs://apollo-phx-nn-ha/user/hive/warehouse/attrib.db/channel_pairs"

    pairs.
      write.
      mode("overwrite").
      parquet(channel_pairs_path)

    val weighted_edges =
      spark.read.parquet(channel_pairs_path).
        groupBy("channel_name", "event_device", "event_type", "previous_channel_name", "previous_event_device", "previous_event_type", "step").
        count().withColumnRenamed("count", "weight")

    weighted_edges.write.mode("overwrite").parquet("hdfs://apollo-phx-nn-ha/user/hive/warehouse/attrib.db/weighted_edges")

    println(s"process ended at ${java.time.Instant.now}")

    /*
     * Stop here copying and pasting to the remote spark-shell to prevent closing your spark session
     */

    spark.stop()
  }

}