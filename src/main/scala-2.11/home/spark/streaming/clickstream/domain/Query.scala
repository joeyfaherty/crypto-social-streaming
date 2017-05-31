package home.spark.streaming.clickstream.domain

/**
  * Created by joey on 31/05/2017.
  */
case class Query(query: String,
                 clickTime: String,
                 timeSpent: String,
                 datePartition: String)


