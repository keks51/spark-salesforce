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


/**
  * Parsing Salesforce record to Spark GenericInternalRow.
  * Getting position of each field from the first record
  * and matching with result spark schema col.
  * For example:
  * Sf Head record cols are (id, name, age)
  * Result schema cols are (name, age, id)
  * Then to match head record with spark schema, cols positions are kept
  * ((name, 1), (age, 2), (id, 0)).
  *
  * While parsing sf record, fields values are getting by position.
  * When col is an offsetCol then this value is stored in SfSparkPartition.
  *
  *
  * @param headRowColNames salesforce record cols
  * @param requiredColsBySchemaOrdering cols that should be get
  * @param sfStreamingPartition partition to store offset value
  * @param offsetColName offset col name
  * @param offsetValueIsString if true then value is wrapped by '.
  *                            For example alex will be 'alex'.
  */
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

  /* If updateOffset = true then offset is stored */
  val parse: (SObject, Boolean) => GenericInternalRow = (sObject: SObject, updateOffset: Boolean) => {
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
