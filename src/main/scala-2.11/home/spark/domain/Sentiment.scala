package home.spark.domain

import org.apache.log4j.Logger

/**
  * Enum for the sentiment of the input text
  */
object Sentiment extends Enumeration {

  val logger: Logger = Logger.getLogger(getClass.getName)

  type Sentiment = Value
  val POSITIVE, NEGATIVE, NEUTRAL = Value

  /**
    * Based on a scoring system where
    * 0 = very negative
    * 1 = negative
    * 2 = neutral
    * 3 = positive
    * 4 = very positive
    * @param sentiment
    * @return
    */
  def scoresToSentiment(sentiment: Int): Sentiment = sentiment match {
    case x if x == 0 || x == 1 => Sentiment.NEGATIVE
    case 2 => Sentiment.NEUTRAL
    case x if x == 3 || x == 4 => Sentiment.POSITIVE
  }
}
