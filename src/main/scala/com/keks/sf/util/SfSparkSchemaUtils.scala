package com.keks.sf.util

import com.keks.sf.exceptions.{NoSuchSalesforceFieldException, UnsupportedSchemaWithSelectException}
import com.keks.sf.implicits.RichStrArray
import org.apache.spark.sql.types.StructType
import org.mule.tools.soql.query.SOQLQuery


object SfSparkSchemaUtils {

  /**
    * Comparing column types defined in .schema(...) with salesforce table column types
    *
    * @param predefinedSchema like .schema(...)
    * @param tableSchema      salesforce table schema
    */
  def comparePredefinedSchemaTypesWithTableSchemaTypes(predefinedSchema: StructType, tableSchema: StructType): Unit = {
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

  /**
    * Creating soql depending on columns selected by SPARK.
    * If salesforce table doesn't contain column selected by spark then an exception is thrown.
    *
    * @param tableName         salesforce table for exception
    * @param sparkSelectedCols like .select(id)
    * @param sfTableColumns    salesforce table columns
    * @param soql              previous soql
    * @return new soql
    */
  def createSoqlFromCols(tableName: String,
                         sparkSelectedCols: Array[String],
                         sfTableColumns: Array[String],
                         soql: SOQLQuery): SOQLQuery = {
    sfTableColumns.containsIgnoreCaseEx(sparkSelectedCols)(elem => throw new NoSuchSalesforceFieldException(tableName, elem, Some(sfTableColumns)))
    SoqlUtils.fillSelectAll(soql, sparkSelectedCols)
  }

  /**
    * Creating Struct Schema and modifying soql.
    *
    * If predefinedSchema schema exist and no columns were selected by SPARK like '.select(id)'
    * then result schema is predefinedSchema schema soql contains only salesforce table columns.
    * For example salesforce table contains id and name but predefinedSchema contains id, name and age.
    * Then age will be null.
    *
    * If predefinedSchema schema exist and some columns were selected SPARK like '.select(id)'
    * then first is to check that selected columns are not equal predefinedSchema.
    * They are equal only if NO FIELDS WERE SELECTED. For example
    * '.format(sfFormat).schema(schema).load(soqlQuery).cache'
    * In case '.format(sfFormat).schema(schema).load(soqlQuery).select(id)'.cache
    * an exception should be thrown since library cannot handle such case.
    *
    * If predefinedSchema schema is empty and no columns were selected by SPARK like '.select(id)'
    * then soqlSchema is used.
    *
    * If predefinedSchema schema is empty and some columns were selected by SPARK like '.select(id)'
    * then schema is created depending on SPARK selected columns
    * and soql will contain only SPARK selected columns. For example soql is 'select id, name from user'
    * and '.select(id)' condition is defined
    * then schema will contain only id and soql will be modified to 'select id from user'
    *
    * @param sparkSelectedCols columns were selected by SPARK like '.select(id)'.
    *                          If no columns were selected like '.select(id)' and option .schema(...) exists
    *                          then 'sparkSelectedCols' contains all columns from .schema(...).
    * @param soql              soql defined in .load(...)
    * @param sfTableName       salesforce table name
    * @param sfTableColNames   salesforce table columns
    * @param soqlSchema        schema based on selected filed in soql query
    * @param predefinedSchema  .schema(...)
    * @return schema and modified soql
    */
  def createSchemaAndModifySoql(sparkSelectedCols: Array[String],
                                soql: SOQLQuery,
                                sfTableName: String,
                                sfTableColNames: Array[String],
                                soqlSchema: StructType,
                                predefinedSchema: Option[StructType]): (StructType, SOQLQuery) = {
    predefinedSchema match {
      case Some(preSchema) =>
        val tightSoql: SOQLQuery = SoqlUtils.fillSelectAll(soql, preSchema.fields.map(_.name).intersectIgnoreCase(sfTableColNames))
        if (sparkSelectedCols.isEmpty) { // counting by spark
          (preSchema, tightSoql)
        } else {
          if (!preSchema.fields.map(_.name).equalsIgnoreCase(sparkSelectedCols)) throw new UnsupportedSchemaWithSelectException()
          (preSchema, tightSoql)
        }
      case None =>
        if (sparkSelectedCols.isEmpty) {
          (soqlSchema, soql)
        } else {
          val tightSchema = StructType(sparkSelectedCols.flatMap(col => soqlSchema.fields.find(_.name.toLowerCase == col.toLowerCase)))
          val resultSoql: SOQLQuery = SfSparkSchemaUtils.createSoqlFromCols(sfTableName, sparkSelectedCols, sfTableColNames, soql)
          (tightSchema, resultSoql)
        }

    }
  }

}
