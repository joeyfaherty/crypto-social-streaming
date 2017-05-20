package home.spark.streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.twitter._
import twitter4j.Status

import scala.io.Source

/**
  * stream tweets in real time and print top 10 hashtags in a sliding window of 1 sec,
  * during a 3 minute window
  */
object TwitterStreamerService {

  def setUpTwitterCredentials(): Unit = {
    // properties file is in the form k=v
    // with keys consumerKey, consumerSecret, accessToken, accessTokenSecret
    for (line <- Source.fromFile("./twitterCredentials.properties").getLines()) {
      val kv = line.split("=")
      if (kv.length == 2) {
        System.setProperty("twitter4j.oauth." + kv(0), kv(1))
      }
    }
  }

  def main(args: Array[String]): Unit = {
    // ensure only error messages are logged
    Logger.getLogger("org").setLevel(Level.ERROR)
    setUpTwitterCredentials()

    val ssc = new StreamingContext("local[*]", getClass.getName, Seconds(1))

    Logger.getRootLogger.setLevel(Level.ERROR)

    // Create a DStream from Twitter using our streaming context
    val tweets: DStream[Status] = TwitterUtils.createStream(ssc, None)
    // Now extract the text of each status update into DStreams using map()
    val hashTags: DStream[String] = tweets.map(status => status.getText)
        // Blow out each word into a new DStream
        .flatMap(tweetText => tweetText.split(" ")).map(s => s.toLowerCase)
        .filter(filterCriteria)

    // Map each hashtag to a key/value pair of (hashtag, 1) so we can count them up by adding up the values
    val hashtagKeyValues: DStream[(String, Int)] = hashTags.map(hashtag => (hashtag, 1))
        // Now count them up over a 5 minute window sliding every one second
        .reduceByKeyAndWindow( (x,y) => x + y, (x,y) => x - y, Seconds(180), Seconds(1))
        // Sort the results by the count values
        .transform(rdd => rdd.sortBy(x => x._2, ascending = false))

    // Print the top 10
    hashtagKeyValues.print

    //** Fault-tolerant (HDFS-like) directory where the checkpoint data will be reliably stored. */
    ssc.checkpoint("./tmp")
    ssc.start()
    ssc.awaitTermination()

  }

  def filterCriteria(word: String): Boolean = {
    // Keep only hashtags
    word.startsWith("#")
    // and bitcoin tweets
    //&& word.contains("btc") || word.contains("bitcoin")
  }

}
