package home.spark.batch.clickstream.domain

case class Query(query: String,
                 clickTime: String,
                 timeSpent: String,
                 datePartition: String)


