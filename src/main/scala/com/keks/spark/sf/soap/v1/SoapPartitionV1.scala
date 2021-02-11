package com.keks.spark.sf.soap.v1

import com.keks.spark.sf.soap.SfSparkPartition
import com.keks.spark.sf.soap.resultset.BatchSoapResultSet
import com.keks.spark.sf.util.{SoqlUtils, UniqueQueryId}
import com.keks.spark.sf.{LogSupport, SfOptions, SfSoapConnection, SfTaskMetrics}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.{Partition, TaskContext}
import org.mule.tools.soql.SOQLParserHelper
import org.mule.tools.soql.query.SOQLQuery


/**
  * Executing partitions on executors.
  *
  * @param soqlFromDriver soql query sent by driver
  * @param sfOptions      spark options wrapped in SfOptions
  * @param schema         result schema
  * @param partitions     array of SfSparkPartition
  * @param spark          sparkSession
  */
class SoapPartitionV1(soqlFromDriver: String,
                      sfOptions: SfOptions,
                      schema: StructType,
                      partitions: Array[SfSparkPartition])(implicit spark: SparkSession,
                                                           queryId: UniqueQueryId)
  extends RDD[InternalRow](spark.sparkContext, Nil) with LogSupport {

  /* just hack */
  override protected def getPartitions: Array[Partition] = partitions.asInstanceOf[Array[Partition]]

  /**
    * Processing spark partition.
    * Getting soql considering partition bounds.
    * Creating SoapBatchSoapResultSet and passing it to iterator.
    *
    * @param split   SfSparkPartition
    * @param context task context to store executor metrics
    */
  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    val soql = SOQLParserHelper.createSOQLData(soqlFromDriver)
    val sfSparkPartition = split.asInstanceOf[SfSparkPartition]
    val partitionId = sfSparkPartition.id
    val partitionedSoql: SOQLQuery = SoqlUtils.addWhereClause(soql, sfSparkPartition.getWhereClause, setInParenthesis = true)

    context.addTaskCompletionListener { _ =>
      infoQ(s"Task Metrics records read: ${context.taskMetrics().inputMetrics.recordsRead}")
      infoQ(s"Task Metrics bytes read: ${context.taskMetrics().inputMetrics.bytesRead}")
      infoQ(s"Partition: $partitionId finished")
    }
    val inputMetrics: SfTaskMetrics = SfTaskMetrics(context.taskMetrics().inputMetrics)
    inputMetrics.commitTime()

    val sfSoapConnection = SfSoapConnection(sfOptions, SoqlUtils.getSoqlTableName(soql), s"PartitionId: $partitionId")
    val soqlStr = partitionedSoql.toSOQLText

    infoQ(s"PartitionId: $partitionId. Soql: $soqlStr")
    val soapResultSet = BatchSoapResultSet(
      sfOptions = sfOptions,
      soql = soqlStr,
      requiredColsBySchemaOrdering = schema.fields.map(e => (e.name, e.dataType)),
      inputMetricsOpt = Some(inputMetrics),
      queryExecutor = sfSoapConnection.queryExecutor,
      sfSparkPartition = sfSparkPartition)

    SoapIteratorV1(soapResultSet)
  }

}
