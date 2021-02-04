package com.keks.sf.util

import com.keks.sf.LogSupport
import com.keks.sf.enums.SoapDelivery
import com.keks.sf.implicits.{RichSOQL, RichTry}
import com.keks.sf.soap.SELECT_ALL_STAR
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

  def getTableNameFromSoql(soqlStr: String): String = {
    new Regex("(?<=(?i)from)(?:\\s*)\\b\\w+\\b")
      .findFirstIn(soqlStr)
      .map(_.trim)
      .getOrElse(throw new RuntimeException(s"Cannot detect table name after 'from' in soql: '$soqlStr'"))
  }

  def isSelectAll(soqlStr: String): Boolean = {
    new Regex(BETWEEN_SELECT_FROM_REGEX).findFirstIn(soqlStr).exists(_.trim == "*")
  }

  def replaceSelectAllStar(soqlStr: String, isSelectAll: Boolean): String = {
    if (isSelectAll) {
      soqlStr.replaceFirst(BETWEEN_SELECT_FROM_REGEX, SELECT_ALL_STAR)
    } else {
      soqlStr
    }
  }

  def fillSelectAll(soql: SOQLQuery, columns: Array[String]): SOQLQuery = {
    val selectClause = new SelectClause()
    columns.foreach(colName => selectClause.addSelectSpec(new FieldSpec(new Field(colName), null)))
    soql.setSelectClause(selectClause)
    soql
  }

  def validateAndParseQuery(soqlStr: String) = {
    val soql: SOQLQuery = Try(SOQLParserHelper.createSOQLData(soqlStr))
      .onFailure { e => println(s"Cannot parse query: '$soqlStr'"); throw e }
    val message: String => String = (option: String) => s"Spark-Salesforce library doesn't support '$option' in soql query"
    require(Option(soql.getGroupByClause).isEmpty, message("group by clause"))
    require(Option(soql.getHavingClause).isEmpty, message("having clause"))
    soql
  }

  def getSoqlSelectFieldNames(soql: SOQLQuery): Array[String] = {
    soql
      .getSelectClause
      .getSelectSpecs
      .asScala
      .map(getFieldName)
      .toArray
  }

  def getFieldName(selectSpec: SelectSpec): String = {
    selectSpec match {
      case field: FieldSpec => field.getField.getFieldName
      case x => throw new IllegalArgumentException(s"Doesn't support soql with: '${x.toSOQLText}'")
    }
  }

  def getSoqlTableName(soql: SOQLQuery): String = {
    soql.getFromClause.getMainObjectSpec.getObjectName
  }

  def addWhereClause(soql: SOQLQuery, s: String, setInParenthesis: Boolean = false): SOQLQuery = {
    val anotherCondition: Condition =
      Try(SOQLParserHelper.createSOQLData(s"SELECT id FROM user WHERE $s").getWhereClause.getCondition)
        .onFailure { e => println(s"Cannot parse where clause: $s"); throw e }
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

  def replaceLowerOffsetBoundOrAddBound(soqlStr: String, value: String, delivery: SoapDelivery): SOQLQuery = {
    val comparisonOperator =
      if (delivery == SoapDelivery.AT_LEAST_ONCE) ComparisonOperator.GREATER_EQUALS else ComparisonOperator.GREATER
    val soql: SOQLQuery = SOQLParserHelper.createSOQLData(soqlStr)
    val errorText: String => String = (reason: String) => s"Cannot set newOffset value in '$soqlStr' because SOQL's \n    $reason"

    val andOperatorRightFunc: PartialFunction[Condition, Condition] = {
      case andOperator: AndOperator => andOperator.getRightCondition
      case _ =>
        val text = errorText(s"WHERE clause doesn't contain AND OPERATOR.")
        println(text)
        throw new IllegalArgumentException(text)
    }

    val parenthesisFunc: PartialFunction[Condition, Condition] = {
      case parenthesis: Parenthesis => parenthesis.getCondition
      case _ =>
        val text = errorText(s"Partition clause is not in Parenthesis")
        println(text)
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

  def getOrderByCols(soql: SOQLQuery): Option[Array[String]] = {
    Option(soql.getOrderByClause)
      .map(_.getOrderBySpecs.asScala.map { orderBySpec =>
        Try(orderBySpec.getOrderByField.asInstanceOf[Field].getFieldName)
          .onFailure { e => println("Only Fields are supported in ORDER BY statement"); throw e }
      }.toArray)
  }

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

  def printSOQL(soqlSqr: String, isSelectAll: Boolean): String = {
    printSOQL(SOQLParserHelper.createSOQLData(soqlSqr), isSelectAll)
  }

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

  def convertToCountQuery(soqlSqr: String): SOQLQuery = {
    convertToCountQuery(SOQLParserHelper.createSOQLData(soqlSqr))
  }
  def convertToCountQuery(soql: SOQLQuery): SOQLQuery = {
    val soqlCopy = soql.copySOQL
    soqlCopy.setOrderByClause(null)
    val countSoqlStr = soqlCopy.toSOQLText.replaceFirst(BETWEEN_SELECT_FROM_REGEX, "count()")
    SOQLParserHelper.createSOQLData(countSoqlStr)
  }

  def changePartitionWhereClause(clauseStr: String, newValue: String): String = {
    val soql = SOQLParserHelper.createSOQLData(s"select id from user $clauseStr")
    val cond = soql.getWhereClause.getCondition.asInstanceOf[AndOperator]
    val k = cond.getLeftCondition.asInstanceOf[FieldBasedCondition]
    k.setLiteral(new Literal(newValue))
    soql.getWhereClause.toSOQLText
  }

}
