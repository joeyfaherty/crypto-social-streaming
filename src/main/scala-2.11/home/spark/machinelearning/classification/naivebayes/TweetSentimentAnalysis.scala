package home.spark.machinelearning.classification.naivebayes

import java.util.Properties

import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations
import edu.stanford.nlp.util.CoreMap
import home.spark.domain.Sentiment
import home.spark.domain.Sentiment.Sentiment
import org.apache.log4j.Logger

import scala.collection.convert.wrapAll._

object TweetSentimentAnalysis {

  val logger: Logger = Logger.getLogger(getClass.getName)

  // creates a StanfordCoreNLP object, with POS tagging, lemmatization, NER,
  // parsing, and coreference resolution
  val props = new Properties()
  props.setProperty("annotators", "tokenize, ssplit, parse, sentiment")
  val pipeline: StanfordCoreNLP = new StanfordCoreNLP(props)

  /**
    * validates the input string and if valid calls extractSentiment
    */
  def mainSentiment(input: String): Sentiment = Option(input) match {
    case Some(text) if text.nonEmpty => extractSentiment(text)
    case _ => throw new IllegalArgumentException("input can't be null or empty")
  }

  /**
    * calls extractSentiments with input text and get max value of sentence.
    * @param text
    * @return Sentiment with the longest (main) sentence
    */
  private def extractSentiment(text: String): Sentiment = {
    val (_, sentiment) = extractSentiments(text)
      .maxBy { case (sentence, _) => sentence.length }
    (sentiment)
  }

  /**
    * Uses Stanford NLP to process the text.
    * @param text
    * @return
    */
  private def extractSentiments(text: String): List[(String, Sentiment)] = {
    val annotation: Annotation = pipeline.process(text)
    val sentences: java.util.List[CoreMap] = annotation.get(classOf[CoreAnnotations.SentencesAnnotation])
    sentences
      .map(sentence => (sentence, sentence.get(classOf[SentimentCoreAnnotations.SentimentAnnotatedTree])))
      .map { case (sentence, tree) => (sentence.toString, Sentiment.scoresToSentiment(RNNCoreAnnotations.getPredictedClass(tree))) }
      .toList
  }

}