package utils

import com.keks.spark.sf.util.UniqueQueryId
import org.apache.spark.sql.SparkSession
import org.mule.tools.soql.SOQLParserHelper
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import utils.xml.SfRecord

import java.sql.Timestamp
import scala.util.{Failure, Success, Try}



class TestBase extends AnyFlatSpec with GivenWhenThen with Matchers {

  implicit val uniqueQueryId: UniqueQueryId = UniqueQueryId(0)
  val CHECKPOINT_DIR = "checkpoint"
  val emptyRecordsList = Seq.empty[Seq[SfRecord]]
  val wireMockServerPort = 18635
  val wireMockServerHost = "localhost"
  val sfServicesEndPoint = "/services/Soap/u"
  val endPoint = "http://localhost:18635"
  val apiVersion = "40.0"
  val sfId = "sf_id_1"
  val sfTableName = "User"
  val SF_TABLE_NAME = "SF_TABLE_NAME"

  implicit lazy val spark: SparkSession = TestBase.initSpark

  def getResourcePath(resourcePath: String): String = {
    getClass.getResource(resourcePath).getPath
  }

  def assertSoqlIsValid(soqlStr: String): Unit ={
    SOQLParserHelper.createSOQLData(soqlStr)
  }

  implicit class TimestampInterpolator(sc: StringContext) {

    def t(arg: Any*): Timestamp = {
      val interpolatedString = sc.s(arg: _*)
      Timestamp.valueOf(interpolatedString)
    }

    def qs(arg: Any*): String = {
      val interpolatedString = sc.s(arg: _*)
      val oneLineStr = interpolatedString.stripMargin.lines.map(_.trim).mkString(" ")
      validateSoql(oneLineStr)
      s"<m:queryString>$oneLineStr</m:queryString>"
    }

    def ql(arg: Any*): String = {
      val interpolatedString = sc.s(arg: _*)
      s"<m:queryLocator>${interpolatedString.stripMargin.lines.map(_.trim).mkString(" ")}</m:queryLocator>"
    }

    private def validateSoql(soql: String): Unit = {
      Try(SOQLParserHelper.createSOQLData(soql.replaceAll("&lt;", "<"))) match {
        case Success(_) =>
        case Failure(exception) =>
          println(s"Cannot parse soql: '$soql'")
          throw exception
      }
    }

  }

}

object TestBase {
  lazy val initSpark: SparkSession = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .config("spark.ui.enabled", "true")
      .config("spark.sql.shuffle.partitions", "10")
      .config("spark.port.maxRetries", "16")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.sql.shuffle.partitions", "20")
      .config("spark.keks.data1", "20")
      .config("keks.data2", "20")
      .appName("Transformations tests")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
//    spark.sparkContext.setLogLevel("TRACE")
    spark
  }
}
