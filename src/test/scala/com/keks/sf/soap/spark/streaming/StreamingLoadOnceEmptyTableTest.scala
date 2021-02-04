package com.keks.sf.soap.spark.streaming

import com.github.tomakehurst.wiremock.client.WireMock.{aResponse, containing, post, urlEqualTo}
import com.github.tomakehurst.wiremock.matching.UrlPattern
import com.keks.sf.SfOptions._
import com.keks.sf.soap.SALESFORCE_SOAP_V2
import org.apache.spark.sql.streaming.OutputMode
import utils.SalesforceColumns.{ID, NAME, SYSTEMMODSTAMP, TIME_FIELD}
import utils.{DataFrameEquality, MockedServer, TestBase, TmpDirectory}
import xml._

import scala.util.Try


class StreamingLoadOnceEmptyTableTest extends TestBase with MockedServer with TmpDirectory with DataFrameEquality {

  "Spark with streaming v2 api" should "load all available data by SystemModstamp" in withMockedServer(wireMockServerHost, wireMockServerPort) { _ =>
    withTempDir { dir =>
      val selectQuery = s"SELECT $ID,$TIME_FIELD,$SYSTEMMODSTAMP FROM $sfTableName"
      val whereQuery = s"WHERE isDeleted = false"
      val bindingXmlResponse = SfBindingXmlResponse(endPoint, apiVersion, sfId).toString
      val describeXmlResponse = SfDescribeXmlResponse(Seq(
        SfField(ID, "string"),
        SfField(SYSTEMMODSTAMP, "datetime"),
        SfField(TIME_FIELD, "time"),
        SfField(NAME, "string"))).toString

      val firstOffset1 = Seq(
        Seq(SfRecord(SYSTEMMODSTAMP, None))
        )
      val sfFirstOffsetXmlResponse1 = SfQueryResultXmlResponse("User", firstOffset1).toString
      val endOffset1 = Seq(
        Seq(SfRecord(SYSTEMMODSTAMP, None))
        )
      val sfEndOffsetXmlResponse1 = SfQueryResultXmlResponse("User", endOffset1).toString


      val bindUrl: UrlPattern = urlEqualTo(s"$sfServicesEndPoint/$apiVersion")
      val requestUrl: UrlPattern = urlEqualTo(s"$sfServicesEndPoint/$apiVersion/$sfId")
      stubSeqOfScenarios("test_scenario")(
        post(bindUrl).willReturn(aResponse().withBody(bindingXmlResponse)),
        post(requestUrl)
          .withRequestBody(containing("</m:describeSObject>"))
          .willReturn(aResponse().withBody(describeXmlResponse)),
        post(requestUrl)
          .withRequestBody(containing(s"<m:queryString>SELECT SystemModstamp FROM User ORDER BY SystemModstamp LIMIT 1</m:queryString>"))
          .willReturn(aResponse().withBody(sfFirstOffsetXmlResponse1)),
        post(requestUrl)
          .withRequestBody(containing(s"<m:queryString>SELECT SystemModstamp FROM User ORDER BY SystemModstamp DESC LIMIT 1</m:queryString>"))
          .willReturn(aResponse().withBody(sfEndOffsetXmlResponse1))
        )
      val streamDF =
        spark
          .readStream
          .option(SF_OFFSET_COL, SYSTEMMODSTAMP)
          .option(SF_STREAMING_QUERY_NAME, "User")
          .option(SF_STREAMING_ADDITIONAL_WAIT_WHEN_INCREMENTAL_LOADING, 60000)
          .option(SF_STREAMING_LOAD_AVAILABLE_DATA, value = true)
          //          .option(SF_STREAMING_INITIAL_OFFSET, "2000-01-01T00:00:00.000Z")
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

      query.processAllAvailable()
      query.stop()

      assert(Try(spark.read.parquet(saveDir)).isFailure)
    }
  }

}
