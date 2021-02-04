package com.keks.sf.soap.v2.streaming

import com.keks.sf.soap.SfSparkPartition
import com.keks.sf.soap.v2.SoapIteratorV2
import com.keks.sf.util.{SerializableSOQLQuery, SfSoapConnection, SoqlUtils}
import com.keks.sf.{LogSupport, SerializableConfiguration, SfOptions}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.{InputPartition, InputPartitionReader}
import org.apache.spark.sql.types.StructType
import org.mule.tools.soql.query.SOQLQuery


class StreamingSoapPartitionV2(soqlSer: SerializableSOQLQuery,
                               sfOptions: SfOptions,
                               schema: StructType,
                               sfSparkPartition: SfSparkPartition,
                               checkpointLocationOpt: String,
                               hdfsConf: SerializableConfiguration)(implicit spark: SparkSession) extends InputPartition[InternalRow] with LogSupport {

  override def createPartitionReader(): InputPartitionReader[InternalRow] = {
    val soql = soqlSer.soql
    val partitionId = sfSparkPartition.id
    val partitionedSoql: SOQLQuery = SoqlUtils.addWhereClause(soql, sfSparkPartition.getWhereClause, setInParenthesis = true)
    val sfSoapConnection = SfSoapConnection(sfOptions, s"PartitionId: $partitionId")
    val soqlStr = partitionedSoql.toSOQLText

    info(s"Partition Id: '$partitionId'. Executor: Querying Sf with: $soqlStr")
    val soapResultSet = StreamingSoapResultSet(
      sfOptions = sfOptions,
      soql = soqlStr,
      requiredColsBySchemaOrdering = schema.fields.map(e => (e.name, e.dataType)),
      queryExecutor = sfSoapConnection.queryExecutor,
      partitionId = partitionId,
      hdfsConf,
      offsetColName = sfOptions.offsetColumn,
      checkpointLocationOpt,
      previousSfStreamingPartition = sfSparkPartition)

    new SoapIteratorV2(soapResultSet)
  }

}
