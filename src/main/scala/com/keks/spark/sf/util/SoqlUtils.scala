package com.keks.spark.sf.util

import com.keks.spark.sf.LogSupport
import com.keks.spark.sf.enums.SoapDelivery
import com.keks.spark.sf.implicits.{RichSOQL, RichTry}
import com.keks.spark.sf.soap.SELECT_ALL_STAR
import org.mule.tools.soql.SOQLParserHelper
import org.mule.tools.soql.query.SOQLQuery
import org.mule.tools.soql.query.clause.{OrderByClause, SelectClause, WhereClause}
import org.mule.tools.soql.query.condition.operator.{AndOperator, ComparisonOperator, Parenthesis}
import org.mule.tools.soql.query.condition.{Condition, FieldBasedCondition}
import org.mule.tools.soql.query.data.{Field, Literal}
import org.mule.tools.soql.query.order.OrderBySpec
import org.mule.tools.soql.query.select.{FieldSpec, SelectSpec}

import scala.collection.JavaConverters._
import scala.util.Try
import scala.util.matching.Regex


object SoqlUtils extends LogSupport {

  val BETWEEN_SELECT_FROM_REGEX = "(?<=(?i)select\\s).*(?=\\s(?i)from)"

  /**
    * Input: 'select * From  User'
    * Output: true
    * Input: 'select id From  User'
    * Output: false
    */
  def isSelectAll(soqlStr: String): Boolean = {
    new Regex(BETWEEN_SELECT_FROM_REGEX).findFirstIn(soqlStr).exists(_.trim == "*")
  }

  /**
    * Replacing * with specific string variable because
    * org.mule.tools.soql library cannot parse *
    *
    */
  def replaceSelectAllStar(soqlStr: String, isSelectAll: Boolean): String = {
    if (isSelectAll) {
      soqlStr.replaceFirst(BETWEEN_SELECT_FROM_REGEX, SELECT_ALL_STAR)
    } else {
      soqlStr
    }
  }

  /**
    * Overriding select statement with array of columns
    * @param soql previous soql query
    * @param columns replace previous select with this columns
    */
  def fillSelectAll(soql: SOQLQuery, columns: Array[String]): SOQLQuery = {
    val selectClause = new SelectClause()
    columns.foreach(colName => selectClause.addSelectSpec(new FieldSpec(new Field(colName), null)))
    soql.setSelectClause(selectClause)
    soql
  }

  /**
    * Validating soql query defined bu User and parsing to SOQLQuery.
    * Check syntax.
    * Group by is prohibited.
    * Having is prohibited.
    *
    * @param soqlStr soql as string to validate and parse
    * @return org.mule.tools.soql.SOQLQuery
    */
  def validateAndParseQuery(soqlStr: String): SOQLQuery = {
    val soql: SOQLQuery = Try(SOQLParserHelper.createSOQLData(soqlStr))
      .onFailure { e => error(s"Cannot parse query: '$soqlStr'"); throw e }
    val message: String => String = (option: String) => s"Spark-Salesforce library doesn't support '$option' in soql query"
    require(Option(soql.getGroupByClause).isEmpty, message("group by clause"))
    require(Option(soql.getHavingClause).isEmpty, message("having clause"))
    soql
  }

  /**
    * Getting fields in select clause.
    * For example, if input 'SELECT id,name FROM User'
    * then result is Array(id, name)
    * @param soql input soql
    * @return Array of columns
    */
  def getSoqlSelectFieldNames(soql: SOQLQuery): Array[String] = {
    soql
      .getSelectClause
      .getSelectSpecs
      .asScala
      .map(getFieldName)
      .toArray
  }

  /* Getting field name from */
  def getFieldName(selectSpec: SelectSpec): String = {
    selectSpec match {
      case field: FieldSpec => field.getField.getFieldName
      case x => throw new IllegalArgumentException(s"Doesn't support soql with: '${x.toSOQLText}'")
    }
  }

  /* Getting table from string soql */
  def getTableNameFromNotParsedSoql(soqlStr: String): String = {
    val replacedSoql = soqlStr.replaceFirst(BETWEEN_SELECT_FROM_REGEX, SELECT_ALL_STAR)
    val soql = SoqlUtils.validateAndParseQuery(replacedSoql)
    getSoqlTableName(soql)
  }

  /**
    * Input: 'select id From  User'
    * Output: User
    */
  def getSoqlTableName(soql: SOQLQuery): String = {
    soql.getFromClause.getMainObjectSpec.getObjectName
  }

  /**
    * Adding where clause to SOQL.
    * For example,
    * Input: 'SELECT Id FROM User'
    * Add: 'WHERE IsDeleted = true'
    * Result: 'SELECT Id FROM User WHERE IsDeleted = true'
    * if setInParenthesis = true then 'SELECT Id FROM User WHERE (IsDeleted = true)'
    *
    * @param soql input SOQL
    * @param stringWhereClause clause to add
    * @param setInParenthesis set in () or not
    */
  def addWhereClause(soql: SOQLQuery,
                     stringWhereClause: String,
                     setInParenthesis: Boolean = false): SOQLQuery = {
    val anotherCondition: Condition =
      Try(SOQLParserHelper.createSOQLData(s"SELECT id FROM user WHERE $stringWhereClause").getWhereClause.getCondition)
        .onFailure { e => error(s"Cannot parse where clause: $stringWhereClause"); throw e }
    Option(soql.getWhereClause)
      .map { clause =>
        val previousCond = if (setInParenthesis) new Parenthesis(clause.getCondition) else clause.getCondition
        val newRightCond = if (setInParenthesis) new Parenthesis(anotherCondition) else anotherCondition
        val newCondition = new AndOperator(previousCond, newRightCond)
        soql.getWhereClause.setCondition(newCondition)
      }.getOrElse {
      if (setInParenthesis) {
        soql.setWhereClause(new WhereClause(new Parenthesis(anotherCondition)))
      } else {
        soql.setWhereClause(new WhereClause(anotherCondition))
      }
    }
    soql
  }

  /**
    * Changing lowerBound offset to new value
    * If SoapDelivery is AT_LEAST_ONCE then lowerBound condition is '>='
    * If SoapDelivery is AT_MOST_ONCE then lowerBound condition is '>'
    * For example.
    * InputSoql: SELECT Id FROM User WHERE (Id >= 10 AND Id <= 20) ORDER BY Id
    * newOffsetValue: 15
    * Delivery: AT_MOST_ONCE
    * Result: SELECT Id FROM User WHERE (Id > 15 AND Id <= 20) ORDER BY Id
    *
    * @param soqlStr soql as string
    * @param value new offset value
    * @param delivery delivery semantic
    * @return
    */
  def replaceLowerOffsetBoundOrAddBound(soqlStr: String, value: String, delivery: SoapDelivery): SOQLQuery = {
    val comparisonOperator =
      if (delivery == SoapDelivery.AT_LEAST_ONCE) ComparisonOperator.GREATER_EQUALS else ComparisonOperator.GREATER
    val soql: SOQLQuery = SOQLParserHelper.createSOQLData(soqlStr)
    val errorText: String => String = (reason: String) => s"Cannot set newOffset value in '$soqlStr' because SOQL's \n    $reason"

    val andOperatorRightFunc: PartialFunction[Condition, Condition] = {
      case andOperator: AndOperator => andOperator.getRightCondition
      case _ =>
        val text = errorText(s"WHERE clause doesn't contain AND OPERATOR.")
        error(text)
        throw new IllegalArgumentException(text)
    }

    val parenthesisFunc: PartialFunction[Condition, Condition] = {
      case parenthesis: Parenthesis => parenthesis.getCondition
      case _ =>
        val text = errorText(s"Partition clause is not in Parenthesis")
        error(text)
        throw new IllegalArgumentException(text)
    }

    val andOperatorLeftFunc: PartialFunction[Condition, Condition] = {
      case andOperator: AndOperator => andOperator.getLeftCondition
    }

    val fieldBasedConditionFunc: PartialFunction[Condition, FieldBasedCondition] = {
      case fieldBasedCondition: FieldBasedCondition => fieldBasedCondition
    }

    val complicatedWhere: PartialFunction[Condition, FieldBasedCondition] = {
      andOperatorRightFunc andThen parenthesisFunc andThen andOperatorLeftFunc andThen fieldBasedConditionFunc
    }

    val onlyPartitionedWhere: PartialFunction[Condition, FieldBasedCondition] = {
      andOperatorLeftFunc andThen fieldBasedConditionFunc
    }


    soql.getWhereClause.getCondition match {
      case _: AndOperator =>
        val cond = complicatedWhere(soql.getWhereClause.getCondition)
        cond.setLiteral(new Literal(value))
        cond.setOperator(comparisonOperator)
      case parenthesis: Parenthesis =>
        val cond = onlyPartitionedWhere(parenthesis.getCondition)
        cond.setLiteral(new Literal(value))
        cond.setOperator(comparisonOperator)
    }

    soql
  }

  /* Input: 'SELECT Id FROM User ORDER BY Id'. Result: Some(Id) */
  def getOrderByCols(soql: SOQLQuery): Option[Array[String]] = {
    Option(soql.getOrderByClause)
      .map(_.getOrderBySpecs.asScala.map { orderBySpec =>
        Try(orderBySpec.getOrderByField.asInstanceOf[Field].getFieldName)
          .onFailure { e => error("Only Fields are supported in ORDER BY statement"); throw e }
      }.toArray)
  }

  /**
    * Input: 'SELECT Id FROM User'
    * colName: Id
    * Result: 'SELECT Id FROM User ORDER BY Id'
    */
  def addOrderByCol(soql: SOQLQuery, colName: String): SOQLQuery = {
    val newOrderBySpec = new OrderBySpec(new Field(colName), null, null)
    Option(soql.getOrderByClause)
      .map { e =>
        e.getOrderBySpecs.add(newOrderBySpec)
      }
      .getOrElse {
        val newOrderByClause = new OrderByClause()
        newOrderByClause.addOrderBySpec(newOrderBySpec)
        soql.setOrderByClause(newOrderByClause)
      }
    soql
  }

  /* pretty print as String. If isSelectAll = true then instead of select cols '*' is used */
  def printSOQL(soqlSqr: String, isSelectAll: Boolean): String = {
    printSOQL(SOQLParserHelper.createSOQLData(soqlSqr), isSelectAll)
  }

  /* pretty print as String. If isSelectAll = true then instead of select cols '*' is used */
  def printSOQL(soql: SOQLQuery, isSelectAll: Boolean): String = {
    if (isSelectAll) {
      soql.toSOQLText.replaceFirst(BETWEEN_SELECT_FROM_REGEX, "*")
    } else {
      val cols = soql
        .getSelectClause
        .getSelectSpecs
        .asScala
        .map { case field: FieldSpec => field.getField.getFieldName }
      val selectStr =
        if (cols.length > 10) {
          cols.take(10).mkString(", ") + ", ..."
        } else {
          cols.mkString(", ")
        }
      soql.toSOQLText.replaceFirst(BETWEEN_SELECT_FROM_REGEX, selectStr)
    }
  }

  /**
    * Input: 'SELECT Id FROM User ORDER BY Id'
    * Output: 'SELECT count() FROM User'
    */
  def convertToCountQuery(soqlSqr: String): SOQLQuery = {
    convertToCountQuery(SOQLParserHelper.createSOQLData(soqlSqr))
  }

  /**
    * Input: 'SELECT Id FROM User ORDER BY Id'
    * Output: 'SELECT count() FROM User'
    */
  def convertToCountQuery(soql: SOQLQuery): SOQLQuery = {
    val soqlCopy = soql.copySOQL
    soqlCopy.setOrderByClause(null)
    val countSoqlStr = soqlCopy.toSOQLText.replaceFirst(BETWEEN_SELECT_FROM_REGEX, "count()")
    SOQLParserHelper.createSOQLData(countSoqlStr)
  }

}
