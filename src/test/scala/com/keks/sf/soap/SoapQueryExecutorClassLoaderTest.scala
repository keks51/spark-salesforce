package com.keks.sf.soap

import com.keks.sf.SfOptions
import com.keks.sf.SfOptions.{SF_API_VERSION, SF_AUTH_END_POINT, SF_USER_NAME, SF_USER_PASSWORD}
import org.apache.spark.sql.DataFrameReader
import org.apache.spark.sql.sources.v2.DataSourceOptions
import utils.TestBase

import scala.collection.mutable


class SoapQueryExecutorClassLoaderTest extends TestBase {

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

  "QueryExecutorClassLoader#SoapQueryExecutor" should "load SoapQueryExecutor" in {
    val className = "com.keks.sf.soap.TrySoapQueryExecutor"
    SoapQueryExecutorClassLoader
      .loadClass(className,
                 sfOptions,
                 null,
                 "Driver")
  }

  "QueryExecutorClassLoader#SoapQueryExecutor" should "fail" in {
    val className = "com.keks.sf.soap.UnknownExecutor"
    assertThrows[ClassNotFoundException](SoapQueryExecutorClassLoader
                                           .loadClass(className,
                                                      sfOptions,
                                                      null,
                                                      "1"))
  }

}
