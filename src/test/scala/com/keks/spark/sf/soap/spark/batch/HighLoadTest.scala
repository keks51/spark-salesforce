package com.keks.spark.sf.soap.spark.batch

import com.github.tomakehurst.wiremock.client.WireMock.{aResponse, containing, post, urlEqualTo}
import com.github.tomakehurst.wiremock.matching.UrlPattern
import com.keks.spark.sf.SfOptions._
import com.keks.spark.sf.soap.DEFAULT_SOAP_QUERY_EXECUTOR_CLASS
import com.keks.spark.sf.{SALESFORCE_SOAP_V1, SALESFORCE_SOAP_V2}
import utils.SalesforceColumns.{ID, NAME, SYSTEMMODSTAMP, TIME_FIELD}
import utils.xml._
import utils.{DataFrameEquality, MockedServer, TestBase}


class HighLoadTest extends TestBase with MockedServer with DataFrameEquality {

  val TIME_VALUE = "13:39:45.000Z"
  val SYSTEMMODSTAMP_VALUE = "2020-10-23T13:39:45.000Z"

  def tests(sfFormat: String): Unit = {
    it should "mock salesforce responses" in withMockedServer(wireMockServerHost, wireMockServerPort) { _ =>
      val soqlQuery = s"SELECT $ID,$SYSTEMMODSTAMP,$TIME_FIELD FROM $sfTableName WHERE isDeleted = false"
      val bindingXmlResponse = SfBindingXmlResponse(endPoint, apiVersion, sfId).toString
      val describeXmlResponse = SfDescribeXmlResponse(Seq(
        SfField(ID, "string"),
        SfField(SYSTEMMODSTAMP, "datetime"),
        SfField(TIME_FIELD, "time"),
        SfField(NAME, "string"))).toString
      val responsesNumber = 10
      val numberOfRecords = 20
      val responses: Seq[String] = (0 to responsesNumber).map { i =>
        val sfRecordsList = (0 until numberOfRecords).map { r =>
          Seq(SfRecord(ID, Some(s"a_${i}_$r")), SfRecord(SYSTEMMODSTAMP, Some(SYSTEMMODSTAMP_VALUE)), SfRecord(TIME_FIELD, Some(TIME_VALUE)))
        }
        i match {
          case 0 =>
            SfQueryResultXmlResponse(sfTableName, sfRecordsList, Some(s"locator_$i")).toString
          case _ if i == responsesNumber =>
            SfQueryResultXmlResponse(sfTableName, sfRecordsList, None, queryMoreResponse = true).toString
          case _ =>
            SfQueryResultXmlResponse(sfTableName, sfRecordsList, Some(s"locator_$i"), queryMoreResponse = true).toString
        }
      }

      val bindUrl: UrlPattern = urlEqualTo(s"$sfServicesEndPoint/$apiVersion")
      val requestUrl: UrlPattern = urlEqualTo(s"$sfServicesEndPoint/$apiVersion/$sfId")

      val posts = (0 until responsesNumber).map { i =>
        println(s"posts: $i")
        post(requestUrl)
          .withRequestBody(containing(ql"locator_$i"))
          .willReturn(aResponse().withBody(responses(i + 1)))
      }

      stubSeqOfScenarios("test_scenario")(
        Seq(
          post(bindUrl).willReturn(aResponse().withBody(bindingXmlResponse)),
          post(requestUrl)
            .withRequestBody(containing("</m:describeSObject>"))
            .willReturn(aResponse().withBody(describeXmlResponse)),
          post(bindUrl).willReturn(aResponse().withBody(bindingXmlResponse)),
          post(requestUrl)
            .withRequestBody(containing(
              qs"""SELECT Id,SystemModstamp,TimeField FROM User
                  |WHERE (isDeleted = false) AND
                  |(SystemModstamp >= 2000-01-01T00:00:00.000Z AND SystemModstamp &lt;= 2020-01-01T00:00:00.000Z)
                  |ORDER BY SystemModstamp""".stripMargin))
            .willReturn(aResponse().withBody(responses.head))) ++ posts: _*
        )


      val resultCount =
        spark
          .read
          .option(SF_INITIAL_OFFSET, "2000-01-01T00:00:00.000Z")
          .option(SF_END_OFFSET, "2020-01-01T00:00:00.000Z")
          .option(SF_USER_NAME, "")
          .option(SOAP_QUERY_EXECUTOR_CLASS_NAME, DEFAULT_SOAP_QUERY_EXECUTOR_CLASS)
          .option(SF_USER_PASSWORD, "")
          .option(SF_USE_HTTPS, value = false)
          .option(SF_AUTH_END_POINT, s"$wireMockServerHost:$wireMockServerPort")
          .option(SF_API_VERSION, apiVersion)
          .option(SF_COMPRESSION, value = false)
          .option(SF_SHOW_TRACE, value = false)
          .option(SF_IS_QUERY_ALL, value = true)
          .format(sfFormat)
          .load(soqlQuery)
          .count

      val expCount = 220
      assert(expCount == resultCount)
    }
  }

  "com.keks.sf.soap.v1" should behave like tests(SALESFORCE_SOAP_V1)
  "com.keks.sf.soap.v2" should behave like tests(SALESFORCE_SOAP_V2)

}
