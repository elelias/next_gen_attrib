/* ordered_path_cooccurrences.scala */
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import scala.collection.mutable
import scala.collection._

object OrderedPathCooccurrencesApp {

 def main(args: Array[String]) {

    val spark = SparkSession.builder.appName("ordered_path_cooccurrences").master("yarn").getOrCreate()
    import spark.implicits._
    import spark.sql

    // Load subsampled dataset
    val txns_df = spark.read.load("hdfs://apollo-phx-nn-ha/user/hive/warehouse/attrib.db/me_txns_correlation_subsample")
    txns_df.createOrReplaceTempView("sample")

    // Select variables of interest with safe guards against null values
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
    coalesce(event_id, "") as event_id,
    event_ts,
    coalesce(channel_name, "Unassigned") as channel_name
    from sample""")

    /*
	UDF to create ordered coocurence pairs for a given marketing path.

	INPUT: Sequence of rows of the form ("event_ts", "event_id", "channel_name") ordered by "event_ts"
	OUTPUT: Sequence of elements of the form (channel_1, channel_2, count)
	PROCESS: - select "event_id" and "channel_name" from each of the rows
		 - combinations(2) creates an iterable of elements the form ((event_id_1, channel_1), (event_id_2, channel_2))
		   for each pair from the list. Since the event_id's are unique, we get entries for multiple pairs of the
		   same channels appearing in the path
		 - turn iterable to List and group by (channel_1, channel_2) to count instances of the pair
		 - convert the result to Seq

    */
    def coocurences(events: Seq[Row]): Seq[(String, String, Int)] = {
       events.map(x => (x.getString(1), x.getString(2))).combinations(2)
        .toList
        .map{case Seq(s1, s2) => (s1,s2)}
        .groupBy(v => (v._1._2, v._2._2))
        .map(x => (x._1._1, x._1._2, x._2.length))
        .toSeq
    }
    
    val coocurences_udf = udf(coocurences _)
    
    /*
	UDF to aggregate all cooccurrence pairs for paths with fixed length.

	INPUT: Sequence of sequences of cooccurence pairs of the form (channel_1, channel_2, cooccurrence).
	       Each sequence corresponds to one path, and each path is represented by sequence of entries in the above format.
	OUTPUT: Summed up cooccurrences for given pairs converted to a semicolon-separated string
    */
    def coocurences_aggregation(events: Seq[Seq[Row]]): String = {
      events.flatMap(l => l)
        .map(x => ((x.getString(0), x.getString(1)), x.getInt(2)))
        .groupBy(v => v._1)
        .mapValues{l => l.map(v => v._2).sum}
        .mkString(";")
    }

    val coocurences_aggregation_udf = udf(coocurences_aggregation _)

    /*
        - group table according to the unique transaction key along with unique transaction info
	- collect all related marketing events to list sorted by event_timestamp
	- create coocurence pairs
	- filter out "short" paths (currently of length <= 1000)
	- aggregate the cooccurrence sequences for each path_length separately
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
      .agg(countDistinct($"event_id") as "path_length",
           sort_array(collect_list(struct("event_ts", "event_id", "channel_name"))) as "mktng_events")
      .filter($"path_length" <= 1000)
      .withColumn("coocurences", coocurences_udf($"mktng_events"))
      .select($"path_length", 
              $"coocurences")
      .groupBy("path_length")
      .agg(coocurences_aggregation_udf(collect_list($"coocurences")) as "coocurence_pairs")
      
    encoded_channels
	.write
	.option("sep","\t")
	.mode("overwrite")
	.csv("hdfs://apollo-phx-nn-ha/user/hive/warehouse/attrib.db/path_analysis/ordered_path_cooccurrences_by_length")

    spark.stop()
  }

}
