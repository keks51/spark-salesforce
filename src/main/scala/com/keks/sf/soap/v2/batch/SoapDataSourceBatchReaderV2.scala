package com.keks.sf.soap.v2.batch

import com.keks.sf.soap.v2.SoapDataSourceReaderV2
import com.keks.sf.util._
import com.keks.sf.{LogSupport, SfOptions, SfSoapConnection}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, InputPartition, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._


/**
  * Spark Batch DataSource Api V2 implementation.
  * Supporting elimination of unneeded columns and filtration using selected predicates.
  * On Driver side:
  * 1) getting the 'offsetColumn' from configuration or taking default (Systemmodstamp).
  * 2) getting the 'initial offset' from conf or salesforce table.
  * 3) getting the 'last offset' from conf or salesforce table.
  * 4) Splitting query in partitions by 'loadNumPartitions' for example '3' and adding an 'order by' clause
  * "select id, name from account where (name = 'alex') AND (age >=  0 AND age < 10) ORDER BY age"
  * "select id, name from account where (name = 'alex') AND (age >= 10 AND age < 20) ORDER BY age"
  * "select id, name from account where (name = 'alex') AND (age >= 20 AND age < 30) ORDER BY age"
  * 5) executing each query on Executor in parallel.
  * Each executor keeps the last offset value from the previous soap batch to continue loading from this offset if an exception occurs.
  * This approach can produce duplicates but guarantee the data consistency.
  *
  * @param sfOptions        spark options wrapped in SfOptions
  * @param predefinedSchema .schema(...)
  * @param spark            spark session
  */
class SoapDataSourceBatchReaderV2(sfOptions: SfOptions,
                                  predefinedSchema: Option[StructType])(implicit spark: SparkSession,
                                                                        uniqueQueryId: UniqueQueryId,
                                                                        sfSoapConnection: SfSoapConnection,
                                                                        parsedSoqlData: ParsedSoqlData)
  extends SoapDataSourceReaderV2(sfOptions, predefinedSchema)
    with DataSourceReader
    with SupportsPushDownRequiredColumns
    with SupportsPushDownFilters
    with LogSupport {

  /**
    * Sending partitions to executors
    */
  override def planInputPartitions(): java.util.List[InputPartition[InternalRow]] = {
    info(s"2Salesforce query is: ${SoqlUtils.printSOQL(soqlToQuery, parsedSoqlData.isSelectAll)}")
    initialPartitions.map { split =>
      new BatchSoapPartitionV2(soqlToQuery,
                               sfOptions,
                               rowSchema,
                               split)
    }.toList.asInstanceOf[List[InputPartition[InternalRow]]].asJava
  }

}
