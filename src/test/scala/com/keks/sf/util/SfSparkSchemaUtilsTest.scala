package com.keks.sf.util

import com.keks.sf.exceptions.{NoSuchSalesforceFieldException, UnsupportedSchemaWithSelectException}
import com.keks.sf.util.SfSparkSchemaUtils._
import org.apache.spark.sql.types._
import org.mule.tools.soql.SOQLParserHelper
import utils.TestBase


class SfSparkSchemaUtilsTest extends TestBase {

  "SfSparkSchemaUtils#comparePredefinedSchemaTypesWithTableSchemaTypes" should "compare schemas" in {
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

    comparePredefinedSchemaTypesWithTableSchemaTypes(preDefined, tableSchema)
  }

  "SfSparkSchemaUtils#comparePredefinedSchemaTypesWithTableSchemaTypes" should "throw exception" in {
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

    assertThrows[IllegalArgumentException](comparePredefinedSchemaTypesWithTableSchemaTypes(preDefined, tableSchema))
  }

  "SfSparkSchemaUtils#createSoqlFromCols" should "create soql" in {
    val soql = SOQLParserHelper.createSOQLData("select id, age from user")
    val res = createSoqlFromCols(
      tableName = sfTableName,
      sparkSelectedCols = Array("id"),
      sfTableColumns = Array("id", "name"),
      soql = soql).toSOQLText
    val exp = "SELECT id FROM user"
    assert(exp == res)
  }

  "SfSparkSchemaUtils#createSoqlFromCols" should "fail" in {
    val soql = SOQLParserHelper.createSOQLData("select id, age from user")

    assertThrows[NoSuchSalesforceFieldException] {
      createSoqlFromCols(
        tableName = sfTableName,
        sparkSelectedCols = Array("id", "salary"),
        sfTableColumns = Array("id", "name"),
        soql = soql)
    }
  }

  "SfSparkSchemaUtils#createSchemaAndModifySoql" should "create schema and soql if sparkSelectedCols == predefinedSchema" in {
    val sparkSelectedCols = Array("id", "age")
    val soql = SOQLParserHelper.createSOQLData("SELECT id, name FROM user")
    val sfTableColNames = Array("id", "name")
    val soqlSchema = StructType(Array(StructField("id", StringType), StructField("name", StringType)))
    val predefinedSchema = StructType(Array(StructField("id", StringType), StructField("age", IntegerType)))

    val (resSchema, resSoql) = createSchemaAndModifySoql(
      sparkSelectedCols = sparkSelectedCols,
      soql = soql,
      sfTableName = sfTableName,
      sfTableColNames = sfTableColNames,
      soqlSchema = soqlSchema,
      predefinedSchema = Some(predefinedSchema))

    assert(predefinedSchema == resSchema)
    val expSoql = SOQLParserHelper.createSOQLData("SELECT id FROM user")
    assert(expSoql.toSOQLText == resSoql.toSOQLText)
  }

  "SfSparkSchemaUtils#createSchemaAndModifySoql" should "create schema and soql if sparkSelectedCols isEmpty" in {
    val sparkSelectedCols = Array.empty[String]
    val soql = SOQLParserHelper.createSOQLData("SELECT id, name FROM user")
    val sfTableColNames = Array("id", "name")
    val soqlSchema = StructType(Array(StructField("id", StringType), StructField("name", StringType)))
    val predefinedSchema = StructType(Array(StructField("id", StringType), StructField("age", IntegerType)))

    val (resSchema, resSoql) = createSchemaAndModifySoql(
      sparkSelectedCols = sparkSelectedCols,
      soql = soql,
      sfTableName = sfTableName,
      sfTableColNames = sfTableColNames,
      soqlSchema = soqlSchema,
      predefinedSchema = Some(predefinedSchema))

    assert(predefinedSchema == resSchema)
    val expSoql = SOQLParserHelper.createSOQLData("SELECT id FROM user")
    assert(expSoql.toSOQLText == resSoql.toSOQLText)
  }

  "SfSparkSchemaUtils#createSchemaAndModifySoql" should "fail schema and soql if sparkSelectedCols != predefinedSchema" in {
    val sparkSelectedCols = Array("id")
    val soql = SOQLParserHelper.createSOQLData("SELECT id, name FROM user")
    val sfTableColNames = Array("id", "name")
    val soqlSchema = StructType(Array(StructField("id", StringType), StructField("name", StringType)))
    val predefinedSchema = StructType(Array(StructField("id", StringType), StructField("age", IntegerType)))

    assertThrows[UnsupportedSchemaWithSelectException] {
      createSchemaAndModifySoql(
        sparkSelectedCols = sparkSelectedCols,
        soql = soql,
        sfTableName = sfTableName,
        sfTableColNames = sfTableColNames,
        soqlSchema = soqlSchema,
        predefinedSchema = Some(predefinedSchema))
    }
  }

  "SfSparkSchemaUtils#createSchemaAndModifySoql" should "return the same schema and soql if predefinedSchema is empty and sparkSelectedCols is empty" in {
    val sparkSelectedCols = Array.empty[String]
    val soql = SOQLParserHelper.createSOQLData("SELECT id, name FROM user")
    val sfTableColNames = Array("id", "name")
    val soqlSchema = StructType(Array(StructField("id", StringType), StructField("name", StringType)))

    val (resSchema, resSoql) = createSchemaAndModifySoql(
      sparkSelectedCols = sparkSelectedCols,
      soql = soql,
      sfTableName = sfTableName,
      sfTableColNames = sfTableColNames,
      soqlSchema = soqlSchema,
      predefinedSchema = None)

    assert(resSchema == soqlSchema)
    assert(soql.toSOQLText == resSoql.toSOQLText)
  }

  "SfSparkSchemaUtils#createSchemaAndModifySoql" should "create schema and soql if predefinedSchema is empty and sparkSelectedCols is non empty" in {
    val sparkSelectedCols = Array("id", "age")
    val soql = SOQLParserHelper.createSOQLData("SELECT id, name FROM user")
    val sfTableColNames = Array("id", "name", "age")
    val soqlSchema = StructType(Array(StructField("id", StringType), StructField("name", StringType)))

    val (resSchema, resSoql) = createSchemaAndModifySoql(
      sparkSelectedCols = sparkSelectedCols,
      soql = soql,
      sfTableName = sfTableName,
      sfTableColNames = sfTableColNames,
      soqlSchema = soqlSchema,
      predefinedSchema = None)
    val expSchema = StructType(Array(StructField("id", StringType)))


    assert(expSchema == resSchema)
    val expSoql = SOQLParserHelper.createSOQLData("SELECT id,age FROM user")
    assert(expSoql.toSOQLText == resSoql.toSOQLText)
  }

}
