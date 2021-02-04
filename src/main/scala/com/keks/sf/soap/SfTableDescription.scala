package com.keks.sf.soap

import com.keks.sf.PartitionSplitter.getOperationType
import com.keks.sf._
import com.keks.sf.enums.SparkNode
import com.keks.sf.exceptions.{NoSuchSalesforceFieldException, UnsupportedSchemaWithSelectException}
import com.keks.sf.implicits.RichStrArray
import com.keks.sf.util.{SfSoapConnection, SfSparkSchemaUtils, SfUtils, SoqlUtils}
import com.sforce.soap.partner.Field
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.mule.tools.soql.query.SOQLQuery

import java.sql.Timestamp


// TODO check sql api like  CREATE TEMPORARY TABLE episodes USING com.databricks.spark.avro OPTIONS (path "src/test/resources/episodes.avro")
trait SfTableDescription extends LogSupport {

  def getSfOptions: SfOptions

  val isSelectAll = getSfOptions.isSelectAll
  private val parsedSoql: SOQLQuery = {
    val rowSoql: String = SoqlUtils.replaceSelectAllStar(getSfOptions.soql, isSelectAll)
    SoqlUtils.validateAndParseQuery(rowSoql)
  }
  val sfTableName = SoqlUtils.getSoqlTableName(parsedSoql)

  val sfSoapConnection = SfSoapConnection(getSfOptions, SparkNode.DRIVER.name)
  private val sfTableFields: Array[Field] = {
    info(s"Getting '$sfTableName' schema from Salesforce")
    sfSoapConnection.describeObject(sfTableName)
  }
  private val sfTableColNames: Array[String] = sfTableFields.map(_.getName)
  private val sfTableDataTypeMap: Map[String, DataType] = sfTableFields.map(SfUtils.parseSfFieldToDataType).toMap


  private val userDefinedSoql: SOQLQuery = {
    if (isSelectAll) {
      SoqlUtils.fillSelectAll(parsedSoql, sfTableDataTypeMap.keys.toArray)
    } else {
      parsedSoql
    }
  }

  private val orderByCols: Option[Array[String]] = SoqlUtils.getOrderByCols(userDefinedSoql)

  val sfTableAsSparkStruct: StructType = {
    val lowerMap: Map[String, DataType] = sfTableDataTypeMap.map { case (key, value) => key.toLowerCase -> value }
    val fields: Array[StructField] = SoqlUtils
      .getSoqlSelectFieldNames(userDefinedSoql).map(e => (e.toLowerCase, e))
      .map { case (colNameLower, colName) =>
        val dataType = lowerMap
          .getOrElse(colNameLower, throw new NoSuchSalesforceFieldException(sfTableName, colName))
        val nullable: Boolean = if (colNameLower == "id") false else true
        StructField(colName, dataType, nullable = nullable)
      }
    StructType(fields)
  }

  val offsetColName = {
    val offsetCol: String = getSfOptions.offsetColumn
    sfTableColNames.containsIgnoreCaseEx(offsetCol)(throw new NoSuchSalesforceFieldException(sfTableName, offsetCol, Some(sfTableColNames), Some("Used For OFFSET")))
    offsetCol
  }

  implicit val partitionColType: PartitionTypeOperations[_ >: Timestamp with Int with Double with String] = {
    val partitionColType = sfTableDataTypeMap.find(_._1.toLowerCase == offsetColName.toLowerCase).map(_._2).get
    getOperationType(partitionColType)
  }

  lazy val initialStartOffset: Option[String] = getSfOptions
    .initialOffsetOpt
    .map { offset => info(s"Taking initial offset from configuration: [$offset]."); offset }
    .orElse {
      sfSoapConnection.getFirstOffsetFromSF(offsetColName, sfTableName)
    }

  lazy val initialEndOffset: Option[String] = getSfOptions
    .sfEndOffsetOpt
    .map { offset => info(s"Taking end offset from configuration: [$offset]."); offset }
    .orElse {
      sfSoapConnection.getLatestOffsetFromSF(offsetColName, sfTableName)
    }

  lazy val initialPartitions: Array[SfSparkPartition] = SfUtils
    .getSparkPartitions(offsetColName,
                        sfTableName,
                        initialStartOffset,
                        initialEndOffset,
                        getSfOptions.sfLoadNumPartitions,
                        isFirstPartitionCondOperatorGreaterAndEqual = true)

  def verifySoqlData(): Unit = {
    orderByCols.foreach(sfTableColNames.containsIgnoreCaseEx(_) { e =>
      throw new NoSuchSalesforceFieldException(sfTableName, e, Some(sfTableColNames), Some("ORDER BY in SOQl"))
    })
    sfTableColNames.containsIgnoreCaseEx(SoqlUtils.getSoqlSelectFieldNames(userDefinedSoql)) { e =>
      throw new NoSuchSalesforceFieldException(sfTableName, e, Some(sfTableColNames), Some("Selected in SOQl"))
    }

  }

  verifySoqlData()

  def getSchemaAndSoql(requiredColumns: Array[String], predefinedSchema: Option[StructType]): (StructType, SOQLQuery) = {
    val (createdSchema, createdSoql) = (predefinedSchema, requiredColumns) match {
      case (Some(preSchema), sparkSelectedCols) if sparkSelectedCols.isEmpty => // counting by spark
        val tightSoql = SfSparkSchemaUtils.getTightSoqlFromSchema(preSchema, sfTableColNames, userDefinedSoql)
        (preSchema, tightSoql)
      case (Some(preSchema), sparkSelectedCols) =>
        if (preSchema.fields.map(_.name.toLowerCase).toSet != sparkSelectedCols.map(_.toLowerCase).toSet)
          throw new UnsupportedSchemaWithSelectException()
        val tightSoql = SfSparkSchemaUtils.getTightSoqlFromSchema(preSchema, sfTableColNames, userDefinedSoql)
        (preSchema, tightSoql)

      case (None, sparkSelectedCols) if sparkSelectedCols.isEmpty =>
        val redundantSchema: StructType = SfSparkSchemaUtils.getRedundantSchemaFromCols(sfTableName, sfTableAsSparkStruct, SoqlUtils.getSoqlSelectFieldNames(userDefinedSoql))
        (redundantSchema, userDefinedSoql)
      case (None, sparkSelectedCols) =>
        val tightSchema = SfSparkSchemaUtils.getTightSchemaFromCols(sfTableAsSparkStruct, sparkSelectedCols)
        val resultSoql = SfSparkSchemaUtils.getSoqlFromCols(sfTableName, sparkSelectedCols, sfTableColNames, userDefinedSoql)
        (tightSchema, resultSoql)
    }
    val withOrderBy = addOrderBy(createdSoql)
    val withOffsetSelect = addOffsetToSelect(withOrderBy)

    (createdSchema, withOffsetSelect)
  }

  private def addOrderBy(soql: SOQLQuery): SOQLQuery = {
    orderByCols match {
      case Some(cols) => if (cols.containsIgnoreCase(offsetColName)) soql else SoqlUtils.addOrderByCol(soql, offsetColName)
      case None => SoqlUtils.addOrderByCol(soql, offsetColName)
    }
  }

  private def addOffsetToSelect(soql: SOQLQuery): SOQLQuery = {
    val selectCols = SoqlUtils.getSoqlSelectFieldNames(soql)
    if (selectCols.map(_.toLowerCase).contains(offsetColName.toLowerCase)) {
      soql
    } else {
      SoqlUtils.fillSelectAll(soql, selectCols :+ offsetColName)
    }
  }

}
