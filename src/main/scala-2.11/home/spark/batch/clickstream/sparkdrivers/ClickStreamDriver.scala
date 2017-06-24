package home.spark.batch.clickstream.sparkdrivers

import home.spark.batch.clickstream.domain.Query
import home.spark.util.SparkSessionUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._


/**
  * csv file in the format:
  * user;sku;category;query;click_time;query_time;city;country;location;time_spent
  */
object ClickStreamDriver {

  val logger: Logger = Logger.getLogger(getClass.getName)

  /**
    * Application entry point to be executed on the cluster
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val env = "production"
    processClickStream(env)
  }

  /**
    * Loads a CSV file with clickstream data, prints the top/bottom 10 queries to the console,
    * persists the data as a parquet file to be accessed for trending, analysis, campaigns.
    * @param env
    */
  def processClickStream(env: String) {
    logger.info("Starting spark driver script for clickstream CSV processing")
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkSession = SparkSessionUtil.getSS(env, getClass.getName)

    import sparkSession.implicits._

    val queryDS = sparkSession.read
      // read these from config
      .option("header", "true")
      .option("sep", ";")
      .csv("../../../Documents/joey/clickstream.csv")
      .sample(withReplacement = true, 0.001) // gives about 500 records
      .map(mapper)
      .cache()

    logger.info("Showing Query Dataset:")
    queryDS.show()

    val dsCount = queryDS.count()
    logger.info(s"DataSet sample count is: + $dsCount")

    logger.info("Printing the top 10 queryDS.... \n")
    val topTenQueries = queryDS.groupBy("query", "datePartition").count().sort($"count".desc)
    topTenQueries.take(10).foreach(r => {
      val query = r(0)
      val count = r(1)
      val date = r(2)
      println(s"Query: $query was run $count times on day $date")
    })

    logger.info("Printing the bottom 10 queryDS.... \n")
    val lastTenQueries = queryDS.groupBy("query", "datePartition").count().sort($"count".asc).take(10)
    lastTenQueries.foreach(r => {
      val query = r(0)
      val count = r(1)
      val date = r(2)
      println(s"Query: $query was run $count times on day $date")
    })

    /** persist clickstream data **/
    val parquetPath = "clickstream"
    val partitionColumn = "datePartition"
    logger.info(s"Writing to parquet table for path: $parquetPath with partition column(s): $partitionColumn")
    queryDS.coalesce(1).write.partitionBy(partitionColumn).mode(SaveMode.Append).parquet(parquetPath)
  }

  def mapper(r: Row): Query = {
    val query = r(3).toString.toLowerCase
    val clickTime = r(4).toString
    val timeSpent = r(9).toString
    val datePartition = clickTime.substring(0, 10)
    Query(
      query,
      clickTime,
      timeSpent,
      datePartition)
  }

}
