package com.keks.sf.soap.v1

import com.keks.sf._
import com.keks.sf.util.{ParsedSoqlData, SoqlUtils, UniqueQueryId}
import org.apache.spark.SparkConf
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}


/**
  * Loading data from Salesforce source using Spark DataSource Api V1
  */
class DefaultSource
  extends RelationProvider
    with SchemaRelationProvider
    with CreatableRelationProvider
    with DataSourceRegister
    with LogSupport {

  implicit val uniqueQueryId: UniqueQueryId = UniqueQueryId.getUniqueQueryId

  /**
    * Creating Salesforce source relation
    *
    * @param sqlContext spark sql context
    * @param parameters spark query parameters like .option(..., ...)
    * @return
    */
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    createRelation(sqlContext, parameters, null)
  }

  /**
    * Creating Salesforce source relation.
    *
    * @param sqlContext spark sql context
    * @param parameters spark query parameters like .option(..., ...)
    * @param schema .schema(...)
    * @return
    */
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType): BaseRelation = {
    val sparkConf: SparkConf = sqlContext.sparkSession.sparkContext.getConf
    val sfOptions = SfOptions(parameters, sparkConf)
    val soqlStr = sfOptions.soql
    val sfTableName = SoqlUtils.getTableNameFromNotParsedSoql(soqlStr)
    implicit val sfSoapConnection: SfSoapConnection = SfSoapConnection(sfOptions = sfOptions, sfTableName, "Driver")
    implicit val parsedSoqlData: ParsedSoqlData = ParsedSoqlData(soqlStr, sfSoapConnection.sfTableDataTypeMap, sfOptions.offsetColumn)
    SoapDataSourceBatchReaderV1(sfOptions, Option(schema))(sqlContext.sparkSession, uniqueQueryId, sfSoapConnection, parsedSoqlData)
  }

  // TODO create save logic
  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], data: DataFrame): BaseRelation = ???

  override def shortName() = "salesforce-soap"

}
