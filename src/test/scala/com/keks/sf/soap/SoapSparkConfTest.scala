package com.keks.sf.soap

import com.keks.sf.SfOptions
import com.keks.sf.SfOptions._
import com.keks.sf.exceptions.SoqlIsNotDefinedException
import org.apache.spark.sql.DataFrameReader
import org.apache.spark.sql.sources.v2.DataSourceOptions
import utils.TestBase

import scala.collection.mutable


class SoapSparkConfTest extends TestBase {

  "Spark" should "set default settings" in {

    val dataFrameReader: DataFrameReader = spark
      .read
      .option(SF_USER_NAME, "user_name")
      .option(SF_USER_PASSWORD, "user_pass")
      .option(SF_AUTH_END_POINT, s"end_point")
      .option(SF_API_VERSION, "apiVersion")


    val field = dataFrameReader.getClass.getDeclaredField("extraOptions")
    field.setAccessible(true)
    val extraOptions: Map[String, String] = field
      .get(dataFrameReader)
      .asInstanceOf[mutable.HashMap[String, String]]
      .toMap ++ Map(DataSourceOptions.PATH_KEY -> "select id, time From User")

    val sfOptions = SfOptions(extraOptions, spark.sparkContext.getConf)

    assert(!sfOptions.compression)
    assert(!sfOptions.showTrace)
    assert(sfOptions.useHttps)
    assert(sfOptions.isQueryAll)
  }

  "Spark" should "failed if SOQL is not set in .load(...)" in {
    assertThrows[SoqlIsNotDefinedException] {
      spark
        .read
        .option(SF_USER_NAME, "")
        .option(SF_USER_PASSWORD, "")
        .option(SF_USE_HTTPS, value = false)
        .option(SF_AUTH_END_POINT, s"end_point")
        .option(SF_API_VERSION, "apiVersion")
        .option(SF_COMPRESSION, value = false)
        .option(SF_SHOW_TRACE, value = false)
        .option(SF_IS_QUERY_ALL, value = true)
        .format(SALESFORCE_SOAP_V1)
        .load()
    }
  }

  "Spark" should "failed if 2 soql queries are set in .load(...)" in {
    assertThrows[IllegalArgumentException] {
      spark
        .read
        .option(SF_USER_NAME, "")
        .option(SF_USER_PASSWORD, "")
        .option(SF_USE_HTTPS, value = false)
        .option(SF_AUTH_END_POINT, s"end_point")
        .option(SF_API_VERSION, "apiVersion")
        .option(SF_COMPRESSION, value = false)
        .option(SF_SHOW_TRACE, value = false)
        .option(SF_IS_QUERY_ALL, value = true)
        .format(SALESFORCE_SOAP_V1)
        .load("select id, time From User", "select id, time From Account")
    }
  }

}
