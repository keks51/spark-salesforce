package com.keks.spark.sf.soap.v2.batch

import com.keks.spark.sf.soap.SfSparkPartition
import com.keks.spark.sf.soap.resultset.BatchSoapResultSet
import com.keks.spark.sf.soap.v2.SoapPartitionV2
import com.keks.spark.sf.util.UniqueQueryId
import com.keks.spark.sf.{SfOptions, SfSoapConnection}
import org.apache.spark.sql.types.StructType


/**
  * Executing partition on executor.
  *
  * @param soqlFromDriver   soql query sent by driver
  * @param sfOptions        spark options wrapped in SfOptions
  * @param schema           result schema
  * @param sfSparkPartition partition
  */
class BatchSoapPartitionV2(soqlFromDriver: String,
                           sfOptions: SfOptions,
                           schema: StructType,
                           sfSparkPartition: SfSparkPartition)
                          (implicit uniqueQueryId: UniqueQueryId) extends SoapPartitionV2(soqlFromDriver = soqlFromDriver,
                                                                                          sfOptions = sfOptions,
                                                                                          sfSparkPartition = sfSparkPartition) {

  /**
    * Creating Batch result set
    *
    * @param partitionedSoqlStr soql
    * @param sfSoapConnection   connection
    * @return
    */
  override def createSoapResultSet(partitionedSoqlStr: String,
                                   sfSoapConnection: SfSoapConnection) = {
    BatchSoapResultSet(
      sfOptions = sfOptions,
      soql = partitionedSoqlStr,
      requiredColsBySchemaOrdering = schema.fields.map(e => (e.name, e.dataType)),
      inputMetricsOpt = None,
      queryExecutor = sfSoapConnection.queryExecutor,
      sfSparkPartition = sfSparkPartition)
  }

}
