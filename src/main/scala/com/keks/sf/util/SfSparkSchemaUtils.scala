package com.keks.sf.util

import com.keks.sf.exceptions.NoSuchSalesforceFieldException
import com.keks.sf.implicits.RichStrArray
import org.apache.spark.sql.types.StructType
import org.mule.tools.soql.query.SOQLQuery


object SfSparkSchemaUtils {

  def getTightSoqlFromSchema(schema: StructType,
                             sfColumns: Array[String],
                             soql: SOQLQuery): SOQLQuery = {
    val sfColumnsLower = sfColumns.map(_.toLowerCase)

    val colsToSelect = schema.fields.map(_.name).filter(schemaCol => sfColumnsLower.contains(schemaCol.toLowerCase))
    SoqlUtils.fillSelectAll(soql, colsToSelect)
  }

  def getRedundantSchemaFromCols(tableName: String,
                                 schema: StructType,
                                 cols: Array[String]): StructType = {
    val schemaFields = schema.fields
    val filteredFields = cols
      .map(col => schemaFields.find(_.name.toLowerCase == col.toLowerCase)
        .getOrElse(throw new NoSuchSalesforceFieldException(tableName, col, Some(cols))))
    StructType(filteredFields)
  }

  def getTightSchemaFromCols(schema: StructType,
                             cols: Array[String]): StructType = {
    val schemaFields = schema.fields
    val filteredFields = cols
      .flatMap(col => schemaFields.find(_.name.toLowerCase == col.toLowerCase))
    StructType(filteredFields)
  }

  def getSoqlFromCols(tableName: String,
                      requiredCols: Array[String],
                      sfTableColumns: Array[String],
                      soql: SOQLQuery): SOQLQuery = {
    sfTableColumns.containsIgnoreCaseEx(requiredCols)(elem => throw new NoSuchSalesforceFieldException(tableName, elem, Some(sfTableColumns)))
    SoqlUtils.fillSelectAll(soql, requiredCols)
  }

  def comparePredefinedSchemaWithTableSchema(predefinedSchema: StructType, tableSchema: StructType): Unit = {
    val tableFields = tableSchema.fields
    val predefinedFields = predefinedSchema.fields
    predefinedFields
      .map(field => tableFields
        .find(_.name.toLowerCase == field.name.toLowerCase)
        .map(tableField =>
               require(
                 tableField.dataType == field.dataType,
                 s"Column: ${field.name}. Type defined in .schema(...) is ${field.dataType} but Salesforce column type as spark sql type is ${tableField.dataType}")))

  }

}
