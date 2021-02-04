package com.keks.sf.util

import com.keks.sf.soap.SfSparkPartition
import com.keks.sf.{LogSupport, PartitionSplitter, PartitionTypeOperations}
import com.sforce.soap.partner.{Field, FieldType}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.mule.tools.soql.query.SOQLQuery

import java.sql.{Date, Timestamp}


object SfUtils extends LogSupport with Serializable {

  def parseSfFieldToDataType(field: Field): (String, DataType) = {
    val name = field.getName
    val fieldType = SfUtils.parseXmlObjTypeToDataType(field.getType.name())
    name -> fieldType
  }

  def parseXmlObjTypeToDataType(xmlObjType: String): DataType = {
    FieldType.valueOf(xmlObjType) match {
      case FieldType.string => StringType
      case FieldType.picklist => StringType
      case FieldType.multipicklist => StringType
      case FieldType.reference => StringType
      case FieldType.combobox => StringType
      case FieldType.base64 => StringType
      case FieldType.textarea => StringType
      case FieldType.currency => StringType
      case FieldType.percent => StringType
      case FieldType.phone => StringType
      case FieldType.id => StringType
      case FieldType.time => StringType
      case FieldType.url => StringType
      case FieldType.email => StringType
      case FieldType.encryptedstring => StringType
      case FieldType.datacategorygroupreference => StringType
      case FieldType.location => StringType
      case FieldType.address => StringType
      case FieldType.anyType => StringType
      case FieldType.complexvalue => StringType
      case FieldType.datetime => TimestampType
      case FieldType.date => StringType
      case FieldType._int => IntegerType
      case FieldType._double => DoubleType
      case FieldType._boolean => BooleanType
      case _ => StringType
    }
  }

  def addFilters(soql: SOQLQuery, filters: Array[Filter]): SOQLQuery = {
    filters.flatMap(compileFilter).foldLeft(soql) { case (soql, filterStr) =>
      println(filterStr)
      SoqlUtils.addWhereClause(soql, s"($filterStr)")
    }
  }

  def compileFilter(filter: Filter): Option[String] = {
    val res = filter match {
      case EqualTo(colName, value) => s"$colName = ${compileFilterValue(value)}"
      case EqualNullSafe(colName, value) => s"$colName = ${compileFilterValue(value)}" // for salesforce we do not need to check if null
      case LessThan(attr, value) => s"$attr < ${compileFilterValue(value)}"
      case GreaterThan(attr, value) => s"$attr > ${compileFilterValue(value)}"
      case LessThanOrEqual(attr, value) => s"$attr <= ${compileFilterValue(value)}"
      case GreaterThanOrEqual(attr, value) => s"$attr >= ${compileFilterValue(value)}"
      case IsNull(attr) => s"$attr = NULL"
      case IsNotNull(attr) => s"$attr != NULL"
      case StringStartsWith(attr, value) => s"$attr LIKE '$value%'"
      case StringEndsWith(attr, value) => s"$attr LIKE '%$value'"
      case StringContains(attr, value) => s"$attr LIKE '%$value%'"
      case In(_, value) if value.isEmpty => null
      case In(attr, value) => s"$attr IN (${compileFilterValue(value)})"
      case Not(f) => compileFilter(f).map(p => s"(NOT ($p))").orNull
      case Or(f1, f2) =>
        // We can't compile Or filter unless both sub-filters are compiled successfully.
        // It applies too for the following And filter.
        // If we can make sure compileFilter supports all filters, we can remove this check.
        val or = Seq(f1, f2).flatMap(compileFilter)
        if (or.size == 2) {
          or.map(p => s"($p)").mkString(" OR ")
        } else {
          null
        }
      case And(f1, f2) =>
        val and = Seq(f1, f2).flatMap(compileFilter)
        if (and.size == 2) {
          and.map(p => s"($p)").mkString(" AND ")
        } else {
          null
        }
      case _ =>
        println(s"Warn. Filter clause '$filter' is not supported")
        null
    }
    Option(res)
  }

  def compileFilterValue(value: Any): Any = value match {
    case stringValue: String => s"'$stringValue'"
    case timestampValue: Timestamp => DateTimeUtils.parseSqlTimestampAsStringToDate(timestampValue.toString)
    case dateValue: Date => s"$dateValue"
    case arrayValue: Array[Any] => arrayValue.map(compileFilterValue).mkString(", ")
    case _ => value
  }

  def getSparkPartitions[T](offsetColName: String,
                            sfTableName: String,
                            lastProcessedOffset: Option[String],
                            endOffset: Option[String],
                            numPartitions: Int,
                            isFirstPartitionCondOperatorGreaterAndEqual: Boolean)
                           (implicit enc: PartitionTypeOperations[T]): Array[SfSparkPartition] = {
    (lastProcessedOffset, endOffset) match {
      case (Some(lowerBound), Some(upperBound)) =>
        if (enc.operationTypeStr == classOf[String].getName) {
          PartitionSplitter.generateSfSparkPartitions(Array((s"$lowerBound", s"$upperBound")), offsetColName, isFirstPartitionCondOperatorGreaterAndEqual)
        } else {
          val bounds = PartitionSplitter.createBounds(lowerBound, upperBound, numPartitions)
          PartitionSplitter.generateSfSparkPartitions(bounds, offsetColName, isFirstPartitionCondOperatorGreaterAndEqual)
        }
      case (None, None) =>
        info(s"Salesforce table: '$sfTableName' is empty")
        Array.empty
    }
  }

}
