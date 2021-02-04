package com.keks.sf.soap.v1

import com.keks.sf._
import com.keks.sf.soap.SfTableDescription
import com.keks.sf.util.{SerializableSOQLQuery, SfSparkSchemaUtils, SfUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}


/**
  *
  * @param sfOptions
  * @param predefinedSchema read.schema(...)
  * @param sparkSession
  */
case class SoapDataSourceBatchReaderV1(sfOptions: SfOptions,
                                       predefinedSchema: Option[StructType])(implicit @transient sparkSession: SparkSession)
  extends BaseRelation
    with SfTableDescription
    with PrunedFilteredScan
    with DataSourceRegister
    with InsertableRelation
    with Serializable {

  override val needConversion: Boolean = false

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val(_, notNullFilters) = filters.partition {
      case _: IsNotNull => true
      case _: IsNull => true
      case _ => false
    }
    val (resultSchema, resultSoql) = getSchemaAndSoql(requiredColumns, predefinedSchema)


    val soqlWithFilters = SfUtils.addFilters(resultSoql, notNullFilters)

    SoapPartitionV1.scanTableByPartitions(SerializableSOQLQuery(soqlWithFilters),
                                          sfOptions,
                                          resultSchema,
                                          initialPartitions).asInstanceOf[RDD[Row]] // because of needConversion: Boolean = false
  }

  override def shortName = "salesforce-soap"

  override def insert(data: DataFrame, overwrite: Boolean): Unit = ???

  override def sqlContext: SQLContext = sparkSession.sqlContext

  override def schema: StructType = {
    predefinedSchema.foreach(SfSparkSchemaUtils.comparePredefinedSchemaWithTableSchema(_, sfTableAsSparkStruct))
    predefinedSchema.getOrElse(sfTableAsSparkStruct)
  }

  override def getSfOptions = sfOptions

}
