package com.keks.spark.sf.soap.v2.streaming

import com.keks.spark.sf.soap.SfSparkPartition
import com.keks.spark.sf.soap.resultset.StreamingSoapResultSet
import com.keks.spark.sf.soap.v2.SoapPartitionV2
import com.keks.spark.sf.util.UniqueQueryId
import com.keks.spark.sf.{SerializableConfiguration, SfOptions, SfSoapConnection}
import org.apache.spark.sql.types.StructType


/**
  * Executing partition on executor.
  *
  * @param soqlFromDriver        soql query sent by driver
  * @param sfOptions             spark options wrapped in SfOptions
  * @param rowSchema                result schema
  * @param sfSparkPartition      partition
  * @param checkpointLocationOpt streaming checkpoint dir location
  * @param hdfsConf              hadoop conf
  */
class StreamingSoapPartitionV2(soqlFromDriver: String,
                               sfOptions: SfOptions,
                               rowSchema: StructType,
                               sfSparkPartition: SfSparkPartition,
                               checkpointLocationOpt: String,
                               hdfsConf: SerializableConfiguration)
                              (implicit queryId: UniqueQueryId)
  extends SoapPartitionV2(soqlFromDriver = soqlFromDriver,
                          sfOptions = sfOptions,
                          sfSparkPartition = sfSparkPartition) {

  /**
    * Creating Streaming result set
    *
    * @param soqlStr          soql
    * @param sfSoapConnection connection
    * @return
    */
  override def createSoapResultSet(soqlStr: String, sfSoapConnection: SfSoapConnection) = {
    StreamingSoapResultSet(
      sfOptions = sfOptions,
      soql = soqlStr,
      requiredColsBySchemaOrdering = rowSchema.fields.map(e => (e.name, e.dataType)),
      queryExecutor = sfSoapConnection.queryExecutor,
      hdfsConf,
      checkpointLocationOpt,
      previousSfStreamingPartition = sfSparkPartition)
  }

}
