package com.keks.sf.util

import com.keks.sf.enums.SoapDelivery
import com.keks.sf.soap.SELECT_ALL_STAR
import com.keks.sf.util.SoqlUtils._
import org.mule.tools.soql.SOQLParserHelper
import org.mule.tools.soql.exception.SOQLParsingException
import org.mule.tools.soql.query.SOQLQuery
import utils.TestBase


class SoqlUtilsTest extends TestBase {

  "SfUtils#isSelectAll" should "return false" in {
    val inputSoql = "select id, time From  User  where id = null"
    val res = isSelectAll(inputSoql)
    assert(!res)
  }

  "SfUtils#isSelectAll" should "return true" in {
    val inputSoql = "select    *   From  User  where id = null"
    val res = isSelectAll(inputSoql)
    assert(res)
  }

  "SfUtils#replaceSelectAllStar" should "replace *" in {
    val inputSoql = "select    *   From  User  where id = null"
    val res = replaceSelectAllStar(inputSoql, isSelectAll = true)
    val exp = s"select $SELECT_ALL_STAR From  User  where id = null"
    assert(exp == res)
  }

  "SfUtils#replaceSelectAllStar" should "don't modify soql" in {
    val inputSoql = "select id, name From User  where id = null"
    val res = replaceSelectAllStar(inputSoql, isSelectAll = false)
    assert(inputSoql == res)
  }

  "SfUtils#fillSelectAll" should "add columns" in {
    val soqlStr = s"select $SELECT_ALL_STAR From  User  where id = null"
    val colNames = Array("id", "Name")
    val res = fillSelectAll(SOQLParserHelper.createSOQLData(soqlStr), colNames).toSOQLText
    val exp = "SELECT id,Name FROM User WHERE id = null"
    assert(exp == res)
  }

  "SfUtils#validateQuery" should "validate without exceptions" in {
    val soql = "select id, name, Data From User where id = null"
    validateAndParseQuery(soql)
  }

  "SfUtils#validateQuery" should "validate without exceptions with where string" in {
    val soql = "select id, name, Data From User where id >= 'keks'"
    validateAndParseQuery(soql)
  }

  "SfUtils#validateQuery" should "validate without exceptions with where integer" in {
    val soql = "select id, name, Data From User where id >= 20"
    validateAndParseQuery(soql)
  }

  "SfUtils#validateQuery" should "validate without exceptions with where double" in {
    val soql = "select id, name, Data From User where id >= 20.0"
    validateAndParseQuery(soql)
  }

  "SfUtils#validateQuery" should "validate without exceptions with alias" in {
    val soql = "select u.id, u.name, u.Data From User u"
    validateAndParseQuery(soql)
  }

  "SfUtils#validateQuery" should "fail if query is incorrect" in {
    val soql = "select count From User where"
    assertThrows[SOQLParsingException](validateAndParseQuery(soql))
  }

  "SfUtils#validateQuery" should "fail with group by validation" in {
    val soql = "select id, count(name) From User group by name"
    assertThrows[IllegalArgumentException](validateAndParseQuery(soql))
  }

  "SfUtils#validateQuery" should "fail if query" in {
    val soql = "select id, name From"
    assertThrows[SOQLParsingException](validateAndParseQuery(soql))
  }

  "SfUtils#getSoqlSelectFieldNames" should "return fields" in {
    val soqlStr = "select id, name, Data From User where id = null"
    val soql: SOQLQuery = SOQLParserHelper.createSOQLData(soqlStr)
    val res = getSoqlSelectFieldNames(soql)
    val exp = List("id", "name", "Data")
    res should contain theSameElementsInOrderAs exp
  }

  "SfUtils#getSoqlSelectFieldNames" should "return fields without alias" in {
    val soqlStr = "select u.id, u.name, u.Data From User u where id = null"
    val soql: SOQLQuery = SOQLParserHelper.createSOQLData(soqlStr)
    val res = getSoqlSelectFieldNames(soql)
    val exp = List("id", "name", "Data")
    res should contain theSameElementsInOrderAs exp
  }

  "SfUtils#getSoqlTableName" should "return fields" in {
    val soqlStr = "select id, name, Data From User where id = null"
    val soql: SOQLQuery = SOQLParserHelper.createSOQLData(soqlStr)
    val res = getSoqlTableName(soql)
    val exp = "User"
    assert(exp == res)
  }

  "SfUtils#getSoqlTableName" should "return fields without alias" in {
    val soqlStr = "select u.id, u.name, u.Data From User u where id = null"
    val soql: SOQLQuery = SOQLParserHelper.createSOQLData(soqlStr)
    val res = getSoqlTableName(soql)
    val exp = "User"
    assert(exp == res)
  }

  "SfUtils#addWhereClause" should "add where clause to other" in {
    val soqlStr = "SELECT id, name, Data FROM User WHERE id = null AND name > 1 AND age < 0"
    val soql: SOQLQuery = SOQLParserHelper.createSOQLData(soqlStr)
    val res = addWhereClause(soql, "age >= 10 AND age <= 20", setInParenthesis = true)
    val exp = "SELECT id,name,Data FROM User WHERE (id = null AND name > 1 AND age < 0) AND (age >= 10 AND age <= 20)"
    assert(exp == res.toSOQLText)
  }

  "SfUtils#addWhereClause" should "add where clause" in {
    val soqlStr = "SELECT id, name, Data FROM User"
    val soql: SOQLQuery = SOQLParserHelper.createSOQLData(soqlStr)
    val res = addWhereClause(soql, "age >= 10 AND age <= 20").toSOQLText
    val exp = "SELECT id,name,Data FROM User WHERE age >= 10 AND age <= 20"
    assert(exp == res)
  }

  "SfUtils#addWhereClause" should "add where clause with Parenthesis" in {
    val soqlStr = "SELECT id, name, Data FROM User"
    val soql: SOQLQuery = SOQLParserHelper.createSOQLData(soqlStr)
    val res = addWhereClause(soql, "age >= 10 AND age <= 20", setInParenthesis = true).toSOQLText
    val exp = "SELECT id,name,Data FROM User WHERE (age >= 10 AND age <= 20)"
    assert(exp == res)
  }

  "SfUtils#replaceLowerOffsetBound" should "replace '2019-01-01T00:00:00.000Z' with 'newValue' in single add" in {
    val soqlStr =
      s"""SELECT id FROM User
         |WHERE
         |(SystemModstamp >= 2019-01-01T00:00:00.000Z AND
         |SystemModstamp < 2019-05-02T16:00:00.000Z) ORDER BY SystemModstamp LIMIT 1""".stripMargin
    val newOffset = "newValue"

    val res = replaceLowerOffsetBoundOrAddBound(soqlStr, newOffset, SoapDelivery.AT_LEAST_ONCE).toSOQLText
    val exp = soqlStr.replaceFirst("2019-01-01T00:00:00.000Z", newOffset).replaceAll("\r\n", " ")
    assert(exp == res)
  }

  "SfUtils#replaceLowerOffsetBound" should "replace '2019-01-01T00:00:00.000Z' with 'newValue'" in {
    val soqlStr =
      s"""SELECT id FROM User
         |WHERE isDeleted = true AND
         |(SystemModstamp > 2019-01-01T00:00:00.000Z AND
         |SystemModstamp < 2019-05-02T16:00:00.000Z) ORDER BY SystemModstamp LIMIT 1""".stripMargin
    val newOffset = "newValue"

    val res = replaceLowerOffsetBoundOrAddBound(soqlStr, newOffset, SoapDelivery.AT_MOST_ONCE).toSOQLText
    val exp = soqlStr.replaceFirst("2019-01-01T00:00:00.000Z", newOffset).replaceAll("\r\n", " ")
    assert(exp == res)
  }

  "SfUtils#replaceLowerOffsetBound" should "throw an exception" in {
    val soqlStr =
      s"""SELECT id FROM User
         |WHERE isDeleted = true AND
         |SystemModstamp >= 2019-01-01T00:00:00.000Z AND
         |SystemModstamp < 2019-05-02T16:00:00.000Z ORDER BY SystemModstamp LIMIT 1""".stripMargin
    val newOffset = "newValue"

    assertThrows[IllegalArgumentException](replaceLowerOffsetBoundOrAddBound(soqlStr, newOffset, SoapDelivery.AT_LEAST_ONCE).toSOQLText)
  }

  "SfUtils#getOrderByCols" should "return cols" in {
    val inputSoql = "SELECT id, time FROM  User  WHERE id = null ORDER BY id, SystemModstamp"
    val soql: SOQLQuery = SOQLParserHelper.createSOQLData(inputSoql)
    val res: Array[String] = getOrderByCols(soql).get
    val exp: Array[String] = Array("id", "SystemModstamp")
    res should contain theSameElementsInOrderAs exp
  }

  "SfUtils#getOrderByCols" should "return None" in {
    val inputSoql = "SELECT id, time FROM  User  WHERE id = null"
    val soql: SOQLQuery = SOQLParserHelper.createSOQLData(inputSoql)
    assert(getOrderByCols(soql).isEmpty)
  }

  "SfUtils#addOrderByCol" should "add col" in {
    val inputSoql = "SELECT id,time FROM User WHERE id = null ORDER BY id"
    val soql: SOQLQuery = SOQLParserHelper.createSOQLData(inputSoql)
    val res = addOrderByCol(soql, "SystemModstamp").toSOQLText
    val exp = "SELECT id,time FROM User WHERE id = null ORDER BY id,SystemModstamp"
    assert(exp == res)
  }

  "SfUtils#addOrderByCol" should "create order by" in {
    val inputSoql = "SELECT id,time FROM User WHERE id = null"
    val soql: SOQLQuery = SOQLParserHelper.createSOQLData(inputSoql)
    val res = addOrderByCol(soql, "SystemModstamp").toSOQLText
    val exp = "SELECT id,time FROM User WHERE id = null ORDER BY SystemModstamp"
    assert(exp == res)
  }

  "SfUtils#printSOQL" should "print *" in {
    val inputSoql = "SELECT id,time FROM User WHERE id = null"
    val soql: SOQLQuery = SOQLParserHelper.createSOQLData(inputSoql)
    val res = printSOQL(soql, isSelectAll = true)
    val exp = "SELECT * FROM User WHERE id = null"
    assert(exp == res)
  }

  "SfUtils#printSOQL" should "print without ..." in {
    val inputSoql = "SELECT a1,a2,a3,a4,a5,a5,a7,a8,a9,a10 FROM User WHERE id = null"
    val soql: SOQLQuery = SOQLParserHelper.createSOQLData(inputSoql)
    val res = printSOQL(soql, isSelectAll = false)
    val exp = "SELECT a1, a2, a3, a4, a5, a5, a7, a8, a9, a10 FROM User WHERE id = null"
    assert(exp == res)
  }

  "SfUtils#printSOQL" should "print with ..." in {
    val inputSoql = "SELECT a1,a2,a3,a4,a5,a5,a7,a8,a9,a10,a11 FROM User WHERE id = null"
    val soql: SOQLQuery = SOQLParserHelper.createSOQLData(inputSoql)
    val res = printSOQL(soql, isSelectAll = false)
    val exp = "SELECT a1, a2, a3, a4, a5, a5, a7, a8, a9, a10, ... FROM User WHERE id = null"
    assert(exp == res)
  }

  "SfUtils#printSOQL" should "return count query without order by" in {
    val inputSoql = "SELECT id,name,age FROM User WHERE id = null ORDER BY id"
    val soql: SOQLQuery = SOQLParserHelper.createSOQLData(inputSoql)
    val res = convertToCountQuery(soql).toSOQLText
    val exp = "SELECT count() FROM User WHERE id = null"
    assert(exp == res)
  }

}
