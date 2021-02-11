package com.keks.spark.sf.soap.v2

import com.keks.spark.sf.soap.v2.batch.SoapDataSourceBatchReaderV2
import com.keks.spark.sf.soap.v2.streaming.SoapDataSourceStreamingReaderV2
import com.keks.spark.sf.util.{ParsedSoqlData, SoqlUtils, UniqueQueryId}
import com.keks.spark.sf.{LogSupport, SfOptions, SfSoapConnection}
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import org.apache.spark.sql.sources.v2.reader.streaming.MicroBatchReader
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, MicroBatchReadSupport, ReadSupport}
import org.apache.spark.sql.types.StructType

import java.util.Optional
import scala.collection.JavaConverters._


/**
  * Loading data from Salesforce source using Spark DataSource Api V2.
  * Supporting Spark Batch and Spark Structured Streaming.
  * Since Spark uses DataSource Api V2 as:
  * 1) Creating Source object while parsing execution plan
  * 2) Creating Source object another one time while parsing executing spark plan
  * so little hack applied to not create the same object 2 times for
  * correct unit testing(don't think less of me, i am just tired of writing unit tests).
  */
class SoapSourceV2
  extends DataSourceV2
    with ReadSupport
    with MicroBatchReadSupport
    with LogSupport {

  // getting spark session
  require(SparkSession.getActiveSession.isDefined)
  implicit val spark: SparkSession = SparkSession.getActiveSession.get
  private var batchReaderOpt: Option[SoapDataSourceBatchReaderV2] = None
  private var streamingReaderOpt: Option[SoapDataSourceStreamingReaderV2] = None

  override def createReader(options: DataSourceOptions): DataSourceReader = {
    createReader(null, options)
  }
  override def createReader(schema: StructType, options: DataSourceOptions): DataSourceReader = {
    batchReaderOpt.map { e =>
      debug("Starting batching...")
      e
    }.getOrElse {
      val sparkConf: SparkConf = spark.sparkContext.getConf
      val sfOptions = SfOptions(options.asMap().asScala.toMap, sparkConf)
      val soqlStr = sfOptions.soql
      val sfTableName = SoqlUtils.getTableNameFromNotParsedSoql(soqlStr)
      implicit val uniqueQueryId: UniqueQueryId = sfOptions.uniqueQueryId
      implicit val sfSoapConnection: SfSoapConnection = SfSoapConnection(sfOptions = sfOptions, sfTableName, "Driver")
      implicit val parsedSoqlData: ParsedSoqlData = ParsedSoqlData(soqlStr, sfSoapConnection.sfTableDataTypeMap, sfOptions.offsetColumn)
      val reader = new SoapDataSourceBatchReaderV2(sfOptions, Option(schema))
      batchReaderOpt = Some(reader)
      reader
    }
  }

  override def createMicroBatchReader(schema: Optional[StructType], checkpointLocation: String, options: DataSourceOptions): MicroBatchReader = {
    streamingReaderOpt.map { e =>
      if (Option(checkpointLocation).isEmpty) throw new IllegalArgumentException("CheckpointLocation cannot be empty")
      e.checkpointLocation = new Path(checkpointLocation).getParent.getParent.toUri.toString
      debug("Starting streaming...")
      e
    }.getOrElse {
      val sparkConf: SparkConf = spark.sparkContext.getConf
      val sfOptions = SfOptions(options.asMap().asScala.toMap, sparkConf)
      val soqlStr = sfOptions.soql
      val sfTableName = SoqlUtils.getTableNameFromNotParsedSoql(soqlStr)
      implicit val uniqueQueryId: UniqueQueryId = sfOptions.uniqueQueryId
      implicit val sfSoapConnection: SfSoapConnection = SfSoapConnection(sfOptions = sfOptions, sfTableName, "Driver")
      implicit val parsedSoqlData: ParsedSoqlData = ParsedSoqlData(soqlStr, sfSoapConnection.sfTableDataTypeMap, sfOptions.offsetColumn)
      val reader = new SoapDataSourceStreamingReaderV2(sfOptions, if (schema.isPresent) Some(schema.get()) else None, checkpointLocation)
      streamingReaderOpt = Some(reader)
      reader
    }
  }
}
