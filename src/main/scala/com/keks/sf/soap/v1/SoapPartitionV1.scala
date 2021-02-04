package com.keks.sf.soap.v1

import com.keks.sf.soap.SfSparkPartition
import com.keks.sf.soap.resultset.SoapBatchSoapResultSet
import com.keks.sf.util.{SerializableSOQLQuery, SfSoapConnection, SoqlUtils}
import com.keks.sf.{LogSupport, SfOptions, SfTaskMetrics}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.mule.tools.soql.query.SOQLQuery


object SoapPartitionV1 extends LogSupport {

  def scanTableByPartitions(soql: SerializableSOQLQuery,
                            sfOptions: SfOptions,
                            requiredSchema: StructType,
                            parts: Array[SfSparkPartition])(implicit spark: SparkSession): RDD[InternalRow] = {
    info(s"Salesforce query is: ${SoqlUtils.printSOQL(soql.soql, sfOptions.isSelectAll)}")
    new SoapPartitionV1(
      spark.sparkContext,
      soql,
      sfOptions,
      requiredSchema,
      parts.asInstanceOf[Array[SfSparkPartition]])
  }

}

class SoapPartitionV1(sc: SparkContext,
                      soqlSer: SerializableSOQLQuery,
                      sfOptions: SfOptions,
                      schema: StructType,
                      partitions: Array[SfSparkPartition])(implicit spark: SparkSession)
  extends RDD[InternalRow](sc, Nil)  with LogSupport {

  override protected def getPartitions: Array[Partition] = partitions.asInstanceOf[Array[Partition]]

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    val soql = soqlSer.soql
    val sfSparkPartition = split.asInstanceOf[SfSparkPartition]
    val partitionId = sfSparkPartition.id
    val partitionedSoql: SOQLQuery = SoqlUtils.addWhereClause(soql, sfSparkPartition.getWhereClause, setInParenthesis = true)

    context.addTaskCompletionListener { _ =>
      info(s"Task Metrics records read: ${context.taskMetrics().inputMetrics.recordsRead}")
      info(s"Task Metrics bytes read: ${context.taskMetrics().inputMetrics.bytesRead}")
      info(s"Partition: $partitionId finished")
    }
    val inputMetrics: SfTaskMetrics = SfTaskMetrics(context.taskMetrics().inputMetrics)
    inputMetrics.commitTime()

    val sfSoapConnection = SfSoapConnection(sfOptions, s"PartitionId: $partitionId")
    val soqlStr = partitionedSoql.toSOQLText

    val offsetField = sfOptions.offsetColumn

    info(s"Querying Sf with: $soqlStr")
    val soapResultSet = SoapBatchSoapResultSet(
      sfOptions = sfOptions,
      soql = soqlStr,
      requiredColsBySchemaOrdering = schema.fields.map(e => (e.name, e.dataType)),
      inputMetricsOpt = Some(inputMetrics),
      queryExecutor = sfSoapConnection.queryExecutor,
      partitionId = partitionId,
      offsetColName = offsetField,
      sfSparkPartition = sfSparkPartition)

    SoapIteratorV1(soapResultSet)
  }

}
