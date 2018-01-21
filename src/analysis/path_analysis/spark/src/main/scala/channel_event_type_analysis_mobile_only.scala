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

    import org.apache.spark.sql.Column

    val valid_transaction: Column = $"transaction_id".isNotNull and $"item_id".isNotNull
    val impression: Column = $"event_type_id" === 4

    val valid_transactions_without_impressions =
      spark.read.load("hdfs://apollo-phx-nn-ha/user/hive/warehouse/attrib.db/me_txns_correlation_subsample").where(valid_transaction and not(impression))

    valid_transactions_without_impressions.
      groupBy($"transaction_id", $"item_id").agg(collect_set($"device_type_level1").as("devices")).where($"devices" === array(lit("Mobile"))).
      select($"transaction_id", $"item_id").join(valid_transactions_without_impressions, Seq("transaction_id", "item_id")).
      write.mode("overwrite").parquet("hdfs://apollo-phx-nn-ha/user/hive/warehouse/attrib.db/mobile_only")

    println(s"process ended at ${java.time.Instant.now}")

    /*
     * Stop here copying and pasting to the remote spark-shell to prevent closing your spark session
     */

    spark.stop()
  }

}