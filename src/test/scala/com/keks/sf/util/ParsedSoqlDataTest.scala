package com.keks.sf.util

import com.keks.sf.exceptions.NoSuchSalesforceFieldException
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.mule.tools.soql.SOQLParserHelper
import utils.TestBase


class ParsedSoqlDataTest extends TestBase {

  "ParsedSoqlData#createSoqlSchema" should "create soql schema" in {
    val inputSoql = SOQLParserHelper
      .createSOQLData("SELECT Id,Name FROM User WHERE id = null ORDER BY id")
    val sfTableDataTypeMap = Map("id" -> StringType, "name" -> StringType, "age" -> IntegerType)

    val res: StructType = ParsedSoqlData.createSoqlSchema("user", sfTableDataTypeMap, inputSoql)
    val exp = StructType(Array(
      StructField("Id", StringType, nullable = false),
      StructField("Name", StringType)
      ))
    assert(exp == res)
  }

  "ParsedSoqlData#createSoqlSchema" should "throw exception" in {
    val inputSoql = SOQLParserHelper
      .createSOQLData("SELECT Id,Name FROM User WHERE id = null ORDER BY id")
    val sfTableDataTypeMap = Map("id" -> StringType, "age" -> IntegerType)
    assertThrows[NoSuchSalesforceFieldException](ParsedSoqlData.createSoqlSchema("user", sfTableDataTypeMap, inputSoql))
  }

}
