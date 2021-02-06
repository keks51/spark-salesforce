package com.keks.sf.util

import com.keks.sf.PartitionSplitter.getOperationType
import com.keks.sf.PartitionTypeOperations
import com.keks.sf.exceptions.NoSuchSalesforceFieldException
import com.keks.sf.implicits.RichStrArray
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.mule.tools.soql.query.SOQLQuery

import java.sql.Timestamp


/**
  * Soql description based on Salesforce table columns.
  *
  * @param soql               SOQL defined by user as org.mule.tools.soql.query.SOQLQuery
  * @param isSelectAll        user used '*' or not
  * @param sfTableDataTypeMap salesforce table columns with spark sql data types
  * @param sfTableColNames    salesforce table columns
  * @param offsetColName      offset col
  * @param sfTableName        Salesforce table name
  * @param soqlSchema         soql query schema
  * @param partitionColType   partition col type like PartitionTypeOperationsIml.intOperations
  * @param orderByCols        order by columns
  */
case class ParsedSoqlData(soql: SOQLQuery,
                          isSelectAll: Boolean,
                          sfTableDataTypeMap: Map[String, DataType],
                          sfTableColNames: Array[String],
                          offsetColName: String,
                          sfTableName: String,
                          soqlSchema: StructType,
                          partitionColType: PartitionTypeOperations[_ >: Timestamp with Int with Double with String],
                          orderByCols: Option[Array[String]])

object ParsedSoqlData {

  def apply(soqlStr: String,
            sfTableDataTypeMap: Map[String, DataType],
            offsetColName: String): ParsedSoqlData = {
    val sfTableCols = sfTableDataTypeMap.keys.toArray
    /* user used '*' or not */
    val isSelectAll = SoqlUtils.isSelectAll(soqlStr)

    // SOQL defined by user as org.mule.tools.soql.query.SOQLQuery
    val parsedSoql: SOQLQuery = {
      val rowSoql: String = SoqlUtils.replaceSelectAllStar(soqlStr, isSelectAll)
      SoqlUtils.validateAndParseQuery(rowSoql)
    }

    // Salesforce table name
    val sfTableName = SoqlUtils.getSoqlTableName(parsedSoql)

    // checking that offset column exists in salesforce
    sfTableCols.containsIgnoreCaseEx(offsetColName)(throw new NoSuchSalesforceFieldException(sfTableName, offsetColName, Some(sfTableCols), Some("Used For OFFSET")))

    // SOQL defined by user like .load(...)
    val userDefinedSoql: SOQLQuery = {
      if (isSelectAll) {
        SoqlUtils.fillSelectAll(parsedSoql, sfTableDataTypeMap.keys.toArray)
      } else {
        parsedSoql
      }
    }

    // Getting order by columns from SOQL
    val orderByCols = SoqlUtils.getOrderByCols(userDefinedSoql)
    val soqlWithOrderBy = orderByCols match {
      case Some(cols) => if (cols.containsIgnoreCase(offsetColName)) userDefinedSoql else SoqlUtils.addOrderByCol(userDefinedSoql, offsetColName)
      case None => SoqlUtils.addOrderByCol(userDefinedSoql, offsetColName)
    }


    val soqlSchema: StructType = createSoqlSchema(sfTableName, sfTableDataTypeMap, soqlWithOrderBy)

    /**
      * Getting TypeOperations implementation for offset col type.
      * If offset col type is Int then PartitionTypeOperationsIml.intOperations is used.
      */
    val partitionColType: PartitionTypeOperations[_ >: Timestamp with Int with Double with String] = {
      val partitionColType = sfTableDataTypeMap.find(_._1.toLowerCase == offsetColName.toLowerCase).map(_._2).get
      getOperationType(partitionColType)
    }

    orderByCols.foreach(sfTableCols.containsIgnoreCaseEx(_) { e =>
      throw new NoSuchSalesforceFieldException(sfTableName, e, Some(sfTableCols), Some("ORDER BY in SOQl"))
    })
    sfTableCols.containsIgnoreCaseEx(SoqlUtils.getSoqlSelectFieldNames(soqlWithOrderBy)) { e =>
      throw new NoSuchSalesforceFieldException(sfTableName, e, Some(sfTableCols), Some("Selected in SOQl"))
    }

    new ParsedSoqlData(soql = soqlWithOrderBy,
                       isSelectAll = isSelectAll,
                       sfTableDataTypeMap = sfTableDataTypeMap,
                       sfTableColNames = sfTableDataTypeMap.keys.toArray,
                       offsetColName = offsetColName,
                       sfTableName = sfTableName,
                       soqlSchema = soqlSchema,
                       partitionColType = partitionColType,
                       orderByCols = orderByCols)
  }

  /**
    * Getting spark struct schema based on select case from SOQL.
    * For example:
    * Soql: 'SELECT Id,Name FROM USER'
    * Result schema: StructType(Array(StructField(Id, StringType), StructField(Name, StringType)))
    *
    * @param sfTableName        table name
    * @param sfTableDataTypeMap salesforce table columns with spark sql data types
    * @param soql               soql
    */
  def createSoqlSchema(sfTableName: String,
                       sfTableDataTypeMap: Map[String, DataType],
                       soql: SOQLQuery) = {
    val lowerMap: Map[String, DataType] = sfTableDataTypeMap.map { case (key, value) => key.toLowerCase -> value }
    val fields: Array[StructField] = SoqlUtils
      .getSoqlSelectFieldNames(soql).map(e => (e.toLowerCase, e))
      .map { case (colNameLower, colName) =>
        val dataType = lowerMap
          .getOrElse(colNameLower, throw new NoSuchSalesforceFieldException(sfTableName, colName))
        val nullable: Boolean = if (colNameLower == "id") false else true
        StructField(colName, dataType, nullable = nullable)
      }
    StructType(fields)
  }

}
