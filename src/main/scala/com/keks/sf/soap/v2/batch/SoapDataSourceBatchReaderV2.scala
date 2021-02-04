package com.keks.sf.soap.v2.batch

import com.keks.sf.soap.v2.{SoapDataSourceReaderV2, SoapPartitionV2}
import com.keks.sf.util._
import com.keks.sf.{LogSupport, SfOptions}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, InputPartition, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.types.StructType
import org.mule.tools.soql.SOQLParserHelper

import scala.collection.JavaConverters._


class SoapDataSourceBatchReaderV2(sfOptions: SfOptions,
                                  predefinedSchema: Option[StructType])(implicit spark: SparkSession)
  extends SoapDataSourceReaderV2(sfOptions, predefinedSchema)
    with DataSourceReader
    with SupportsPushDownRequiredColumns
    with SupportsPushDownFilters
    with LogSupport {


  override def getSfOptions = sfOptions


  override def planInputPartitions(): java.util.List[InputPartition[InternalRow]] = {

    val soqlWithFilters = {SOQLParserHelper.createSOQLData(soqlWithDefaultFilters)}
    info(s"2Salesforce query is: ${SoqlUtils.printSOQL(soqlWithFilters, isSelectAll)}")
    val data: List[InputPartition[InternalRow]] = initialPartitions.map { split =>
      new SoapPartitionV2(SerializableSOQLQuery(soqlWithFilters),
                          sfOptions,
                          resultSchema,
                          split)
    }.toList
    data.asJava
  }

}
