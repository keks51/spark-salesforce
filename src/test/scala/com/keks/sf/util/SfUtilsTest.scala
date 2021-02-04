package com.keks.sf.util


import com.keks.sf.util.SfUtils.{addFilters, compileFilter}
import org.apache.spark.sql.sources.{And => AND, _}
import org.mule.tools.soql.SOQLParserHelper
import utils.TestBase


class SfUtilsTest extends TestBase {

  val applyFilterToSoql = (filter: String) => s"SELECT id FROM user WHERE $filter"

  "SfUtils#compileFilter" should "compile EqualTo" in {
    val filter = EqualTo("name", "alex")
    val filterStr = compileFilter(filter).get
    val soqlStr = applyFilterToSoql(filterStr)
    assertSoqlIsValid(soqlStr)
  }

  "SfUtils#compileFilter" should "compile EqualNullSafe" in {
    val filter = EqualNullSafe("name", "alex")
    val filterStr = compileFilter(filter).get
    val soqlStr = applyFilterToSoql(filterStr)
    assertSoqlIsValid(soqlStr)
  }

  "SfUtils#compileFilter" should "compile LessThan with timestamp" in {
    val filter = LessThan("createdDate", t"2020-10-23 16:39:45")
    val filterStr = compileFilter(filter).get
    val soqlStr = applyFilterToSoql(filterStr)
    assertSoqlIsValid(soqlStr)
  }

  "SfUtils#compileFilter" should "compile LessThan with date" in {
    val filter = LessThan("createdDate", t"2020-10-23 16:39:45")
    val filterStr = compileFilter(filter).get
    val soqlStr = applyFilterToSoql(filterStr)
    assertSoqlIsValid(soqlStr)
  }

  "SfUtils#compileFilter" should "compile LessThan with Integer" in {
    val filter = LessThan("count", 10)
    val filterStr = compileFilter(filter).get
    val soqlStr = applyFilterToSoql(filterStr)
    assertSoqlIsValid(soqlStr)
  }

  "SfUtils#compileFilter" should "compile GreaterThan with Integer" in {
    val filter = GreaterThan("count", 10)
    val filterStr = compileFilter(filter).get
    val soqlStr = applyFilterToSoql(filterStr)
    assertSoqlIsValid(soqlStr)
  }

  "SfUtils#compileFilter" should "compile LessThanOrEqual with Integer" in {
    val filter = LessThanOrEqual("count", 10)
    val filterStr = compileFilter(filter).get
    val soqlStr = applyFilterToSoql(filterStr)
    assertSoqlIsValid(soqlStr)
  }

  "SfUtils#compileFilter" should "compile GreaterThanOrEqual with Integer" in {
    val filter = GreaterThanOrEqual("count", 10)
    val filterStr = compileFilter(filter).get
    val soqlStr = applyFilterToSoql(filterStr)
    assertSoqlIsValid(soqlStr)
  }

  "SfUtils#compileFilter" should "compile IsNull" in {
    val filter = IsNull("name")
    val filterStr = compileFilter(filter).get
    val soqlStr = applyFilterToSoql(filterStr)
    assertSoqlIsValid(soqlStr)
  }

  "SfUtils#compileFilter" should "compile IsNotNull" in {
    val filter = IsNotNull("name")
    val filterStr = compileFilter(filter).get
    val soqlStr = applyFilterToSoql(filterStr)
    assertSoqlIsValid(soqlStr)
  }

  "SfUtils#compileFilter" should "compile StringStartsWith" in {
    val filter = StringStartsWith("name", "a")
    val filterStr = compileFilter(filter).get
    val soqlStr = applyFilterToSoql(filterStr)
    assertSoqlIsValid(soqlStr)
  }

  "SfUtils#compileFilter" should "compile StringEndsWith" in {
    val filter = StringEndsWith("name", "a")
    val filterStr = compileFilter(filter).get
    val soqlStr = applyFilterToSoql(filterStr)
    assertSoqlIsValid(soqlStr)
  }

  "SfUtils#compileFilter" should "compile StringContains" in {
    val filter = StringContains("name", "a")
    val filterStr = compileFilter(filter).get
    val soqlStr = applyFilterToSoql(filterStr)
    assertSoqlIsValid(soqlStr)
  }

  "SfUtils#compileFilter" should "compile In" in {
    val filter = In("name", Array("a", "b", "c"))
    val filterStr = compileFilter(filter).get
    val soqlStr = applyFilterToSoql(filterStr)
    assertSoqlIsValid(soqlStr)
  }

  "SfUtils#compileFilter" should "return None if In when values are empty" in {
    val filter = In("name", Array.empty)
    assert(compileFilter(filter).isEmpty)
  }

  "SfUtils#compileFilter" should "compile Not" in {
    val filter = Not(EqualTo("name", "alex"))
    val filterStr = compileFilter(filter).get
    val soqlStr = applyFilterToSoql(filterStr)
    assertSoqlIsValid(soqlStr)
  }

  "SfUtils#compileFilter" should "compile Or" in {
    val filter = Or(EqualTo("name", "alex"), EqualTo("name", "bob"))
    val filterStr = compileFilter(filter).get
    val soqlStr = applyFilterToSoql(filterStr)
    assertSoqlIsValid(soqlStr)
  }

  "SfUtils#compileFilter" should "compile And" in {
    val filter = AND(EqualTo("name", "alex"), EqualTo("age", 30))
    val filterStr = compileFilter(filter).get
    val soqlStr = applyFilterToSoql(filterStr)
    assertSoqlIsValid(soqlStr)
  }

  "SfUtils#compileFilter1" should "compile And" in {
    val filter = AND(EqualTo("name", "alex"), EqualTo("age", 30))
    val filterStr = compileFilter(filter).get
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

}
