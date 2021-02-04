package com.keks.sf.soap

import com.keks.sf.implicits.RichTry
import com.keks.sf.soap.SoapUtils.convertXmlObjectToXmlFieldsArray
import com.keks.sf.util.DateTimeUtils
import com.sforce.soap.partner.sobject.SObject
import com.sforce.ws.bind.XmlObject
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import java.math.BigDecimal
import java.sql.Date
import scala.util.Try


case class SalesforceRecordParser(headRowColNames: Array[String],
                                  requiredColsBySchemaOrdering: Array[(String, DataType)],
                                  sfStreamingPartition: SfSparkPartition,
                                  offsetColName: String,
                                  offsetValueIsString: Boolean) {

  private val castToSparkSqlDataType = (value: String, toType: (String, DataType)) => {
    val (colName, colType) = toType
    Try {
      colType match {
        case _: ByteType => value.toByte
        case _: ShortType => value.toShort
        case _: IntegerType => value.toInt
        case _: LongType => value.toLong
        case _: FloatType => value.toFloat
        case _: DoubleType => value.toDouble
        case _: BooleanType => value.toBoolean
        case _: DecimalType => new BigDecimal(value.replaceAll(",", ""))
        case _: TimestampType => DateTimeUtils.isoStrToTimestamp(value).getTime * 1000
        case _: DateType => Date.valueOf(value)
        case _: StringType => UTF8String.fromString(value)
        case _ => throw new IllegalArgumentException(s"Unsupported data type: ${colType.typeName} for column: $colName")
      }
    }.onFailure { exception =>
      throw new IllegalArgumentException(s"Cannot parse value: '$value' to Type: '${colType.typeName}' for column: $colName", exception)
    }
  }
  private val sfColsLower: Array[String] = headRowColNames.map(_.toLowerCase)
  private val offsetColPos = sfColsLower.indexOf(offsetColName.toLowerCase)
  private val requiredColsWithSfColPos: Array[((String, DataType), Int)] = requiredColsBySchemaOrdering
    .map(e => (e, sfColsLower.indexOf(e._1.toLowerCase)))

  val parse = (sObject: SObject, updateOffset: Boolean) => {
    val colValues: Array[XmlObject] = convertXmlObjectToXmlFieldsArray(sObject)
    val data: Array[Any] = {
      if (colValues.isEmpty) {
        Array.empty[Any]
      } else {
        requiredColsWithSfColPos.map { case (colNameAndDataType, sfColPos) =>
          if (sfColPos == -1) {
            null
          } else {
            Option(colValues(sfColPos).getValue).map { sfValue =>
              if (updateOffset && offsetColPos == sfColPos) {
                sfStreamingPartition.setOffset(if (offsetValueIsString) s"'$sfValue'" else sfValue.toString)
              }
              castToSparkSqlDataType(sfValue.toString, colNameAndDataType)
            }.orNull
          }
        }
      }
    }
    new GenericInternalRow(data)
  }

}
