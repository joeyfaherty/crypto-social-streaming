package home.spark.streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * find the top 10 growing (popular) and top 10 declining queries executed by users.
  *
  * csv file in the format:
  * user;sku;category;query;click_time;query_time;city;country;location;time_spent
  */
object ClickStreamService {

  // user;sku;category;query;click_time;query_time;city;country;location;time_spent
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val ss = SparkSession.builder()
      .appName(getClass.getName)
      .master("local[*]")
      .getOrCreate()

    val linesDF = ss.read
      .option("header", "true")
      .option("sep", ";")
      .csv("../../../Documents/joey/kaggle_geo.csv").cache()

    import ss.implicits._

    println("Printing the top 10 queries.... ")
    val topTenQueries = linesDF.groupBy("query").count().sort($"count".desc).take(10)
    topTenQueries.foreach(r => {
      val query = r(0)
      val count = r(1)
      println(s"Query: $query was run $count times")
    })

    println("Printing the bottom 10 queries.... ")
    val lastTenQueries = linesDF.groupBy("query").count().sort($"count".asc).take(10)
    lastTenQueries.foreach(r => {
      val query = r(0)
      val count = r(1)
      println(s"Query: $query was run $count times")
    })
  }

}
