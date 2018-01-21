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

    val transactions_sample = spark.read.load("hdfs://apollo-phx-nn-ha/user/hive/warehouse/attrib.db/me_txns_correlation_subsample")

    val valid_transaction: Column = $"transaction_id".isNotNull and $"item_id".isNotNull
    val click: Column = $"event_type_id".isin(1, 5, 7, 8)
    val open: Column = $"event_type_id" === 6
    val impression: Column = $"event_type_id" === 4

    val partition_by_checkout_order_by_timestamp: WindowSpec =
      Window.partitionBy($"transaction_id", $"item_id").orderBy($"event_ts".desc)

    val marketing_info = transactions_sample.select(
      $"transaction_id",
      $"item_id",
      $"event_ts",
      struct(
        when(click, "click").when(open, "open").otherwise("undefined").as("event_type"),
        coalesce($"device_type_level1", lit("unassigned")).as("event_device"),
        coalesce($"channel_name", lit("unassigned")).as("channel_name")
      ).as("current")
    ).where(valid_transaction and not(impression))

    val pairs = marketing_info.
      withColumn("previous", lag("current", 1, null).over(partition_by_checkout_order_by_timestamp)).
      withColumn("step", rank().over(partition_by_checkout_order_by_timestamp))

    val weighted_edges =
      pairs.
        groupBy("current", "previous", "step").
        count().withColumnRenamed("count", "weight")

    weighted_edges.write.mode("overwrite").parquet("hdfs://apollo-phx-nn-ha/user/hive/warehouse/attrib.db/weighted_edges")

    println(s"process ended at ${java.time.Instant.now}")

    /*
     * Stop here copying and pasting to the remote spark-shell to prevent closing your spark session
     */

    spark.stop()
  }

}