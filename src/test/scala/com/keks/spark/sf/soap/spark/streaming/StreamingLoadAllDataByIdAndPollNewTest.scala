package com.keks.spark.sf.soap.spark.streaming

import com.github.tomakehurst.wiremock.client.WireMock
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.matching.UrlPattern
import com.keks.spark.sf.SALESFORCE_SOAP_V2
import com.keks.spark.sf.SfOptions._
import org.apache.spark.sql.streaming.OutputMode
import utils.SalesforceColumns.{ID, SYSTEMMODSTAMP}
import utils.xml._
import utils.{DataFrameEquality, MockedServer, TestBase, TmpDirectory}

import scala.util.{Failure, Try}


class StreamingLoadAllDataByIdAndPollNewTest extends TestBase with MockedServer with DataFrameEquality with TmpDirectory {

  import spark.implicits._

  "Spark with streaming v2 api" should "load all available data and poll new" in withMockedServer(wireMockServerHost, wireMockServerPort) { _ =>
    withTempDir { dir =>
      val selectQuery = s"SELECT $ID FROM $sfTableName"
      val whereQuery = s"WHERE isDeleted = false"
      val orderBySoql = s"ORDER BY $ID"
      val bindingXmlResponse = SfBindingXmlResponse(endPoint, apiVersion, sfId).toString
      val describeXmlResponse = SfDescribeXmlResponse(Seq(
        SfField(ID, "string")
        )).toString

      val endOffset1 = Seq(
        Seq(SfRecord(SYSTEMMODSTAMP, Some("h")))
        )
      val sfEndOffsetXmlResponse1 = SfQueryResultXmlResponse("User", endOffset1).toString

      val sfRecordsList1 = Seq(
        Seq(SfRecord(ID, Some("a"))),
        Seq(SfRecord(ID, Some("b")))
        )
      val sfQueryResultXmlResponse1 = SfQueryResultXmlResponse("User", sfRecordsList1, Some("locator_1"), globalRecordsNumber = 8).toString

      val sfRecordsList2 = Seq(
        Seq(SfRecord(ID, Some("c"))),
        Seq(SfRecord(ID, Some("d")))
        )
      val sfQueryResultXmlResponse2 = SfQueryResultXmlResponse("User", sfRecordsList2, Some("locator_2"), queryMoreResponse = true).toString

      val sfRecordsList3 = Seq(
        Seq(SfRecord(ID, Some("e"))),
        Seq(SfRecord(ID, Some("f")))
        )
      val sfQueryResultXmlResponse3 = SfQueryResultXmlResponse("User", sfRecordsList3, Some("locator_3"), queryMoreResponse = true).toString

      val sfRecordsList4 = Seq(
        Seq(SfRecord(ID, Some("g"))),
        Seq(SfRecord(ID, Some("h")))
        )
      val sfQueryResultXmlResponse4 = SfQueryResultXmlResponse("User", sfRecordsList4, None, queryMoreResponse = true).toString

      val sfQueryResultXmlResponse5 = SfQueryResultXmlResponse("User", emptyRecordsList, None, globalRecordsNumber = 0).toString
      val sfQueryResultXmlResponse6 = SfQueryResultXmlResponse("User", emptyRecordsList, None).toString
      val endOffset2 = Seq(
        Seq(SfRecord(SYSTEMMODSTAMP, Some("p")))
        )
      val sfEndOffsetXmlResponse2 = SfQueryResultXmlResponse("User", endOffset2).toString
      val sfRecordsList7 = Seq(
        Seq(SfRecord(ID, Some("i"))),
        Seq(SfRecord(ID, Some("j")))
        )
      val sfQueryResultXmlResponse7 = SfQueryResultXmlResponse("User", sfRecordsList7, Some("locator_4"), globalRecordsNumber = 8).toString

      val sfRecordsList8 = Seq(
        Seq(SfRecord(ID, Some("k"))),
        Seq(SfRecord(ID, Some("l")))
        )
      val sfQueryResultXmlResponse8 = SfQueryResultXmlResponse("User", sfRecordsList8, Some("locator_5"), queryMoreResponse = true).toString

      val sfRecordsList9 = Seq(
        Seq(SfRecord(ID, Some("m"))),
        Seq(SfRecord(ID, Some("n")))
        )
      val sfQueryResultXmlResponse9 = SfQueryResultXmlResponse("User", sfRecordsList9, Some("locator_6"), queryMoreResponse = true).toString

      val sfRecordsList10 = Seq(
        Seq(SfRecord(ID, Some("o"))),
        Seq(SfRecord(ID, Some("p")))
        )
      val sfQueryResultXmlResponse10 = SfQueryResultXmlResponse("User", sfRecordsList10, None, queryMoreResponse = true).toString

      val sfQueryResultXmlResponse11 = SfQueryResultXmlResponse("User", emptyRecordsList, None, globalRecordsNumber = 0).toString

      val bindUrl: UrlPattern = urlEqualTo(s"$sfServicesEndPoint/$apiVersion")
      val requestUrl: UrlPattern = urlEqualTo(s"$sfServicesEndPoint/$apiVersion/$sfId")
      stubSeqOfScenarios("test_scenario")(
        post(bindUrl).willReturn(aResponse().withBody(bindingXmlResponse)),
        post(requestUrl)
          .withRequestBody(containing("</m:describeSObject>"))
          .willReturn(aResponse().withBody(describeXmlResponse)),
        post(requestUrl)
          .withRequestBody(containing(s"<m:queryString>SELECT Id FROM User ORDER BY Id DESC LIMIT 1</m:queryString>"))
          .willReturn(aResponse().withBody(sfEndOffsetXmlResponse1)),
        post(bindUrl).willReturn(aResponse().withBody(bindingXmlResponse)),
        post(requestUrl)
          .withRequestBody(containing(s"<m:queryString>$selectQuery WHERE (isDeleted = false) AND ($ID >= 'a' AND $ID &lt;= 'h') $orderBySoql</m:queryString>"))
          .willReturn(aResponse().withBody(sfQueryResultXmlResponse1)),
        post(requestUrl)
          .withRequestBody(containing(s"<m:queryLocator>locator_1</m:queryLocator>"))
          .willReturn(aResponse().withBody(sfQueryResultXmlResponse2)),
        post(bindUrl).willReturn(aResponse().withBody(bindingXmlResponse)),
        post(requestUrl)
          .withRequestBody(containing(s"<m:queryLocator>locator_2</m:queryLocator>"))
          .willReturn(aResponse().withBody(sfQueryResultXmlResponse3)),
        post(requestUrl)
          .withRequestBody(containing(s"<m:queryLocator>locator_3</m:queryLocator>"))
          .willReturn(aResponse().withBody(sfQueryResultXmlResponse4)),
        post(requestUrl)
          .withRequestBody(containing(s"<m:queryString>SELECT count() FROM User WHERE (isDeleted = false) AND (Id > 'h')</m:queryString>"))
          .willReturn(aResponse().withBody(sfQueryResultXmlResponse5)),
        post(requestUrl)
          .withRequestBody(containing(s"<m:queryString>SELECT count() FROM User WHERE (isDeleted = false) AND (Id > 'h')</m:queryString>"))
          .willReturn(aResponse().withBody(sfQueryResultXmlResponse5)),
        post(requestUrl)
          .withRequestBody(containing(s"<m:queryString>SELECT count() FROM User WHERE (isDeleted = false) AND (Id > 'h')</m:queryString>"))
          .willReturn(aResponse().withBody(sfQueryResultXmlResponse6)),
        post(requestUrl)
          .withRequestBody(containing(s"<m:queryString>SELECT Id FROM User ORDER BY Id DESC LIMIT 1</m:queryString>"))
          .willReturn(aResponse().withBody(sfEndOffsetXmlResponse2)),
        post(bindUrl).willReturn(aResponse().withBody(bindingXmlResponse)),
        post(requestUrl)
          .withRequestBody(containing(s"<m:queryString>$selectQuery WHERE (isDeleted = false) AND ($ID > 'h' AND $ID &lt;= 'p') $orderBySoql</m:queryString>"))
          .willReturn(aResponse().withBody(sfQueryResultXmlResponse7)),
        post(requestUrl)
          .withRequestBody(containing(s"<m:queryLocator>locator_4</m:queryLocator>"))
          .willReturn(aResponse().withBody(sfQueryResultXmlResponse8)),
        post(bindUrl).willReturn(aResponse().withBody(bindingXmlResponse)),
        post(requestUrl)
          .withRequestBody(containing(s"<m:queryLocator>locator_5</m:queryLocator>"))
          .willReturn(aResponse().withBody(sfQueryResultXmlResponse9)),
        post(requestUrl)
          .withRequestBody(containing(s"<m:queryLocator>locator_6</m:queryLocator>"))
          .willReturn(aResponse().withBody(sfQueryResultXmlResponse10)),
        post(requestUrl)
          .withRequestBody(containing(s"<m:queryString>SELECT count() FROM User WHERE (isDeleted = false) AND (Id > 'p')</m:queryString>"))
          .willReturn(aResponse().withBody(sfQueryResultXmlResponse11)),
        post(requestUrl)
          .withRequestBody(containing(s"<m:queryString>SELECT count() FROM User WHERE (isDeleted = false) AND (Id > 'p')</m:queryString>"))
          .willReturn(aResponse().withBody(sfQueryResultXmlResponse11))
        )



      val streamDF =
        spark
          .readStream
          .option(SF_OFFSET_COL, ID)
          .option(SF_STREAMING_QUERY_NAME, "User")
          .option(SF_STREAMING_ADDITIONAL_WAIT_WHEN_INCREMENTAL_LOADING, 100)
          .option(SF_STREAMING_LOAD_AVAILABLE_DATA, value = false)
          .option(SF_INITIAL_OFFSET, "a")
          .option(SF_STREAMING_MAX_BATCHES, 2)
          .option(SF_USER_NAME, "")
          .option(SF_USER_PASSWORD, "")
          .option(SF_USE_HTTPS, value = false)
          .option(SF_AUTH_END_POINT, s"$wireMockServerHost:$wireMockServerPort")
          .option(SF_API_VERSION, apiVersion)
          .option(SF_COMPRESSION, value = false)
          .option(SF_SHOW_TRACE, value = false)
          .option(SF_IS_QUERY_ALL, value = true)
          .format(SALESFORCE_SOAP_V2)
          .load(selectQuery + " " + whereQuery)


      val saveDir = s"$dir/result"
      val query = streamDF.writeStream
        .outputMode(OutputMode.Append)
        .format("parquet")
        .option("path", saveDir)
        .option("checkpointLocation", s"$dir/$CHECKPOINT_DIR")
        .start()

      Try {
        query.awaitTermination()
        query.stop()

      } match {
        case Failure(ex: org.apache.spark.sql.streaming.StreamingQueryException) if ex.getMessage.contains("Failed to get next element") =>
      }

      val result = spark.read.parquet(saveDir)
      result.show(false)
      val expected = Seq(
        "a",
        "b",
        "c",
        "d",
        "e",
        "f",
        "g",
        "h",
        "i",
        "j",
        "k",
        "l",
        "m",
        "n",
        "o",
        "p"
        ).toDF(ID)
      verify(WireMock.exactly(5), postRequestedFor(bindUrl))
      verify(WireMock.exactly(17), postRequestedFor(requestUrl)) // +1 for last bad request
      assertDataFramesEqual(expected, result)
    }
  }

}
