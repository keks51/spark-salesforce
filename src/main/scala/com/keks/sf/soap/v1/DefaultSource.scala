package com.keks.sf.soap.v1

import com.keks.sf._
import org.apache.spark.SparkConf
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}


class DefaultSource
  extends RelationProvider
    with SchemaRelationProvider
    with CreatableRelationProvider
    with DataSourceRegister
    with LogSupport {

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    createRelation(sqlContext, parameters, null)
  }

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType): BaseRelation = {
    val sparkConf: SparkConf = sqlContext.sparkSession.sparkContext.getConf
    val sfOptions = SfOptions(parameters, sparkConf)
    SoapDataSourceBatchReaderV1(sfOptions, Option(schema))(sqlContext.sparkSession)
  }

  // TODO create save logic
  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], data: DataFrame): BaseRelation = ???

  override def shortName() = "salesforce-soap"

}
