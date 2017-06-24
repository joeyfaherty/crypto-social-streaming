package home.spark.machinelearning

import java.io.StringReader

import scala.collection.mutable

/**
  * Created by joey on 22/06/2017.
  */
object TweetTokenizer {

/*  def tokenize(content: String): Seq[String] = {
    val tReader = new StringReader(content)
    val analyzer = new EnglishAnalyzer(LuceneVersion)
    val tStream = analyzer.tokenStream("contents", tReader)
    val term = tStream.addAttribute(classOf[CharTermAttribute])
    tStream.reset()

    val result = mutable.ArrayBuffer.empty[String]
    while(tStream.incrementToken()) {
      val termValue = term.toString
      if (!(termValue matches ".*[\\d\\.].*")) {
        result += term.toString
      }
    }
    result
  }*/

}
