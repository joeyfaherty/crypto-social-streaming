package home.spark.util

import org.apache.spark.sql.SparkSession

/**
  * Created by joey on 22/06/2017.
  */
object SparkSessionUtil {

  /**
    * selects which SparkSession to use. Either local for unit tests or
    * any other string will create a SparkSession with cluster configuration
    *
    * @param env
    * @return
    */
  def getSS(env: String, appName: String): SparkSession = env match {
    case "local" => SparkSession
      .builder()
      // run locally utilizing all cores
      .master("local[*]")
      .appName(appName)
      .getOrCreate()
    case _ => SparkSession
      .builder()
      // don't set .master() when running on the cluster, as Spark will then use the default cluster pre-configuration
      .appName(getClass.getName)
      .getOrCreate()
  }

}
