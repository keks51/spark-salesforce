package com.keks.spark.sf.soap.v2

import com.keks.spark.sf.soap.SfSparkPartition
import com.keks.spark.sf.util.{SoqlUtils, UniqueQueryId}
import com.keks.spark.sf.{LogSupport, SfOptions, SfResultSet, SfSoapConnection}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.{InputPartition, InputPartitionReader}
import org.mule.tools.soql.SOQLParserHelper
import org.mule.tools.soql.query.SOQLQuery


/**
  * Executing sf partition on executor.
  * Soap resultSet should be provided.
  *
  * @param soqlFromDriver          soql query sent by driver
  * @param sfOptions        spark options wrapped in SfOptions
  * @param sfSparkPartition partition
  */
abstract class SoapPartitionV2(soqlFromDriver: String,
                               sfOptions: SfOptions,
                               sfSparkPartition: SfSparkPartition)
                              (implicit uniqueQueryId: UniqueQueryId) extends InputPartition[InternalRow] with LogSupport {

  /**
    * Result set
    * @param soqlStr soql
    * @param sfSoapConnection connection
    * @return
    */
  def createSoapResultSet(soqlStr: String,
                          sfSoapConnection: SfSoapConnection): SfResultSet

  /**
    * Processing spark partition.
    * Getting soql considering partition bounds.
    * Creating BatchSoapResultSet and passing it to iterator.
    */
  override def createPartitionReader(): InputPartitionReader[InternalRow] = {
    val soql: SOQLQuery = SOQLParserHelper.createSOQLData(soqlFromDriver)
    val partitionId: Int = sfSparkPartition.id
    val partitionedSoql: SOQLQuery = SoqlUtils.addWhereClause(soql, sfSparkPartition.getWhereClause, setInParenthesis = true)
    val sfSoapConnection = SfSoapConnection(sfOptions, SoqlUtils.getSoqlTableName(soql), s"PartitionId: $partitionId")
    val soapResultSet = createSoapResultSet(partitionedSoql.toSOQLText, sfSoapConnection)
    new SoapIteratorV2(soapResultSet)
  }

}
