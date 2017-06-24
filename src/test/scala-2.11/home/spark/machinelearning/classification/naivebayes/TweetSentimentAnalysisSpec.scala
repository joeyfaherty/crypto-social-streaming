package home.spark.machinelearning.classification.naivebayes

import home.spark.domain.Sentiment
import org.scalatest.{FunSpec, Matchers}

class TweetSentimentAnalysisSpec extends FunSpec with Matchers {

  describe("tweet analysis") {

    it("should return POSITIVE") {
      val input = "I played football in the sun twice because I like it."
      val sentiment = TweetSentimentAnalysis.mainSentiment(input)
      sentiment should be(Sentiment.POSITIVE)
    }

    it("should return NEGATIVE") {
      val input = "I hate all football."
      val sentiment = TweetSentimentAnalysis.mainSentiment(input)
      sentiment should be(Sentiment.NEGATIVE)
    }

    it("should return NEUTRAL") {
      val input = "I am reading a book"
      val sentiment = TweetSentimentAnalysis.mainSentiment(input)
      sentiment should be(Sentiment.NEUTRAL)
    }

    it("should throw an IllegalArgumentException exception when input is empty") {
      intercept[IllegalArgumentException] {
        val input = ""
        TweetSentimentAnalysis.mainSentiment(input)
      }
    }

  }
}

