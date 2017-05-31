package home.spark.streaming.clickstream

import java.io.File

import home.spark.streaming.clickstream.sparkdrivers.ClickStreamDriver
import org.apache.commons.io.FileUtils
import org.scalatest.{BeforeAndAfter, FunSuite}

/**
  * Created by joey on 30/05/2017.
  */
class ClickStreamDriverTest extends FunSuite with BeforeAndAfter {

  /* clean up after test */
  after {
    FileUtils.deleteDirectory(new File("clickstream"))
  }

  test("Simple app test") {
    val env = "local"
    ClickStreamDriver.processClickStream(env)
  }

}
