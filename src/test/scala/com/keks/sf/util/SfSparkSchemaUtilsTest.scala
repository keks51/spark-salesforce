package com.keks.sf.util

import org.apache.spark.sql.types._
import utils.TestBase


class SfSparkSchemaUtilsTest extends TestBase {

  "SfUtils#comparePredefinedSchemaWithTableSchema" should "compare schemas" in {
    val preDefined = StructType(Array(
      StructField("Id", StringType),
      StructField("name", StringType),
      StructField("Age", IntegerType),
      StructField("systemmodstamp", TimestampType)
      ))

    val tableSchema = StructType(Array(
      StructField("id", StringType),
      StructField("age", IntegerType),
      StructField("Systemmodstamp", TimestampType),
      StructField("lastmodifieddate", TimestampType)
      ))

    SfSparkSchemaUtils.comparePredefinedSchemaWithTableSchema(preDefined, tableSchema)
  }

  "SfUtils#comparePredefinedSchemaWithTableSchema" should "throw exception" in {
    val preDefined = StructType(Array(
      StructField("Id", TimestampType),
      StructField("name", StringType),
      StructField("Age", IntegerType),
      StructField("systemmodstamp", TimestampType)
      ))

    val tableSchema = StructType(Array(
      StructField("id", StringType),
      StructField("age", IntegerType),
      StructField("Systemmodstamp", TimestampType),
      StructField("lastmodifieddate", TimestampType)
      ))

    assertThrows[IllegalArgumentException](SfSparkSchemaUtils.comparePredefinedSchemaWithTableSchema(preDefined, tableSchema))
  }

}
