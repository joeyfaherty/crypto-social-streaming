package home.spark.batch.clickstream

import java.io.File

import home.spark.batch.clickstream.sparkdrivers.ClickStreamDriver
import org.apache.commons.io.FileUtils
import org.scalatest.{BeforeAndAfter, FunSuite}

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
