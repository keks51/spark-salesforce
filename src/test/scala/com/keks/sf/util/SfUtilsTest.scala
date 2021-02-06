package com.keks.sf.util


import com.keks.sf.soap.{ExecutorMetrics, SfSparkPartition}
import com.keks.sf.util.SfUtils.{addFilters, compileFilterValue, parseFilter}
import org.apache.spark.Partition
import org.apache.spark.sql.sources.{And => AND, _}
import org.mule.tools.soql.SOQLParserHelper
import org.scalatest.enablers.Aggregating
import utils.TestBase

import scala.collection.GenTraversable


class SfUtilsTest extends TestBase {

  val applyFilterToSoql = (filter: String) => s"SELECT id FROM user WHERE $filter"

  "SfUtils#compileFilter" should "compile EqualTo" in {
    val filter = EqualTo("name", "alex")
    val filterStr = parseFilter(filter).get
    val soqlStr = applyFilterToSoql(filterStr)
    assertSoqlIsValid(soqlStr)
  }

  "SfUtils#compileFilter" should "compile EqualNullSafe" in {
    val filter = EqualNullSafe("name", "alex")
    val filterStr = parseFilter(filter).get
    val soqlStr = applyFilterToSoql(filterStr)
    assertSoqlIsValid(soqlStr)
  }

  "SfUtils#compileFilter" should "compile LessThan with timestamp" in {
    val filter = LessThan("createdDate", t"2020-10-23 16:39:45")
    val filterStr = parseFilter(filter).get
    val soqlStr = applyFilterToSoql(filterStr)
    assertSoqlIsValid(soqlStr)
  }

  "SfUtils#compileFilter" should "compile LessThan with date" in {
    val filter = LessThan("createdDate", t"2020-10-23 16:39:45")
    val filterStr = parseFilter(filter).get
    val soqlStr = applyFilterToSoql(filterStr)
    assertSoqlIsValid(soqlStr)
  }

  "SfUtils#compileFilter" should "compile LessThan with Integer" in {
    val filter = LessThan("count", 10)
    val filterStr = parseFilter(filter).get
    val soqlStr = applyFilterToSoql(filterStr)
    assertSoqlIsValid(soqlStr)
  }

  "SfUtils#compileFilter" should "compile GreaterThan with Integer" in {
    val filter = GreaterThan("count", 10)
    val filterStr = parseFilter(filter).get
    val soqlStr = applyFilterToSoql(filterStr)
    assertSoqlIsValid(soqlStr)
  }

  "SfUtils#compileFilter" should "compile LessThanOrEqual with Integer" in {
    val filter = LessThanOrEqual("count", 10)
    val filterStr = parseFilter(filter).get
    val soqlStr = applyFilterToSoql(filterStr)
    assertSoqlIsValid(soqlStr)
  }

  "SfUtils#compileFilter" should "compile GreaterThanOrEqual with Integer" in {
    val filter = GreaterThanOrEqual("count", 10)
    val filterStr = parseFilter(filter).get
    val soqlStr = applyFilterToSoql(filterStr)
    assertSoqlIsValid(soqlStr)
  }

  "SfUtils#compileFilter" should "compile IsNull" in {
    val filter = IsNull("name")
    val filterStr = parseFilter(filter).get
    val soqlStr = applyFilterToSoql(filterStr)
    assertSoqlIsValid(soqlStr)
  }

  "SfUtils#compileFilter" should "compile IsNotNull" in {
    val filter = IsNotNull("name")
    val filterStr = parseFilter(filter).get
    val soqlStr = applyFilterToSoql(filterStr)
    assertSoqlIsValid(soqlStr)
  }

  "SfUtils#compileFilter" should "compile StringStartsWith" in {
    val filter = StringStartsWith("name", "a")
    val filterStr = parseFilter(filter).get
    val soqlStr = applyFilterToSoql(filterStr)
    assertSoqlIsValid(soqlStr)
  }

  "SfUtils#compileFilter" should "compile StringEndsWith" in {
    val filter = StringEndsWith("name", "a")
    val filterStr = parseFilter(filter).get
    val soqlStr = applyFilterToSoql(filterStr)
    assertSoqlIsValid(soqlStr)
  }

  "SfUtils#compileFilter" should "compile StringContains" in {
    val filter = StringContains("name", "a")
    val filterStr = parseFilter(filter).get
    val soqlStr = applyFilterToSoql(filterStr)
    assertSoqlIsValid(soqlStr)
  }

  "SfUtils#compileFilter" should "compile In" in {
    val filter = In("name", Array("a", "b", "c"))
    val filterStr = parseFilter(filter).get
    val soqlStr = applyFilterToSoql(filterStr)
    assertSoqlIsValid(soqlStr)
  }

  "SfUtils#compileFilter" should "return None if In when values are empty" in {
    val filter = In("name", Array.empty)
    assert(parseFilter(filter).isEmpty)
  }

  "SfUtils#compileFilter" should "compile Not" in {
    val filter = Not(EqualTo("name", "alex"))
    val filterStr = parseFilter(filter).get
    val soqlStr = applyFilterToSoql(filterStr)
    assertSoqlIsValid(soqlStr)
  }

  "SfUtils#compileFilter" should "compile Or" in {
    val filter = Or(EqualTo("name", "alex"), EqualTo("name", "bob"))
    val filterStr = parseFilter(filter).get
    val soqlStr = applyFilterToSoql(filterStr)
    assertSoqlIsValid(soqlStr)
  }

  "SfUtils#compileFilter" should "compile And" in {
    val filter = AND(EqualTo("name", "alex"), EqualTo("age", 30))
    val filterStr = parseFilter(filter).get
    val soqlStr = applyFilterToSoql(filterStr)
    assertSoqlIsValid(soqlStr)
  }

  "SfUtils#compileFilter1" should "compile And" in {
    val filter = AND(EqualTo("name", "alex"), EqualTo("age", 30))
    val filterStr = parseFilter(filter).get
    val soqlStr = applyFilterToSoql(filterStr)
    assertSoqlIsValid(soqlStr)
  }

  "SfUtils#addFilters" should "return the same soql" in {
    val soql = SOQLParserHelper.createSOQLData("SELECT id FROM user")
    val res = addFilters(soql, Array.empty)
    assert(soql.toSOQLText == res.toSOQLText)
  }

  "SfUtils#addFilters" should "return soql with additional filters" in {
    val soql = SOQLParserHelper.createSOQLData("SELECT id FROM user")
    val filters: Array[Filter] = Array(
      GreaterThan("createdDate", t"2020-10-10 01:05:24"),
      Or(EqualTo("name", "alex"), EqualTo("name", "bob")),
      LessThan("createdDate", t"2020-10-10 03:18:44"))

    val res = addFilters(soql, filters).toSOQLText
    val exp = "SELECT id FROM user WHERE (createdDate > 2020-10-10T01:05:24.000Z) AND ((name = 'alex') OR (name = 'bob')) AND (createdDate < 2020-10-10T03:18:44.000Z)"
    assert(res == exp)
  }

  "SfUtils#compileFilterValue" should "compile String" in {
    val inputValue: Any = "alex"

    val res = compileFilterValue(inputValue)
    val exp = "'alex'"
    assert(exp == res)
  }

  "SfUtils#compileFilterValue" should "compile TimestampType" in {
    val inputValue: Any = t"2020-01-01 00:00:00"

    val res = compileFilterValue(inputValue).toString
    val exp = "2020-01-01T00:00:00.000Z"
    assert(exp == res)
  }

  "SfUtils#compileFilterValue" should "compile Array of String" in {
    val inputValue: Any = Array("alex", "bob")

    val res = compileFilterValue(inputValue).toString
    val exp = "'alex','bob'"
    assert(exp == res)
  }

  "SfUtils#compileFilterValue" should "compile Int" in {
    val inputValue: Any = 123

    val res = compileFilterValue(inputValue).toString
    val exp = "123"
    assert(exp == res)
  }

  "SfUtils#getSparkPartitions" should "create partitions partitioned by Int" in {
    import com.keks.sf.PartitionTypeOperationsIml.intOperations

    val offsetColName = "age"
    val res = SfUtils.getSparkPartitions(
      offsetColName = offsetColName,
      sfTableName = sfTableName,
      lastProcessedOffset = Some("10"),
      endOffset = Some("20"),
      numPartitions = 2,
      isFirstPartitionCondOperatorGreaterAndEqual = true)
    val exp = Array(
      SfSparkPartition(0, "age", "10", "15", ">=", "<", offsetValueIsString = false, ExecutorMetrics(offset = "10")),
      SfSparkPartition(1, "age", "15", "20", ">=", "<=", offsetValueIsString = false, ExecutorMetrics(offset = "15"))
    )
    Array(exp) should contain theSameElementsAs Array(res)
  }

  "SfUtils#getSparkPartitions" should "return empty array" in {
    import com.keks.sf.PartitionTypeOperationsIml.intOperations

    val offsetColName = "age"
    val res = SfUtils.getSparkPartitions(
      offsetColName = offsetColName,
      sfTableName = sfTableName,
      lastProcessedOffset = None,
      endOffset = None,
      numPartitions = 2,
      isFirstPartitionCondOperatorGreaterAndEqual = true)
    assert(res.isEmpty)
  }

}
