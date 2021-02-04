package com.keks.sf.soap.v2

import com.keks.sf.soap.v2.batch.SoapDataSourceBatchReaderV2
import com.keks.sf.soap.v2.streaming.SoapDataSourceStreamingReaderV2
import com.keks.sf.{LogSupport, SfOptions}
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import org.apache.spark.sql.sources.v2.reader.streaming.MicroBatchReader
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, MicroBatchReadSupport, ReadSupport}
import org.apache.spark.sql.types.StructType

import java.util.Optional
import scala.collection.JavaConverters._


class SoapSourceV2 extends DataSourceV2 with ReadSupport with MicroBatchReadSupport with LogSupport {

  assert(SparkSession.getActiveSession.isDefined)
  implicit val spark: SparkSession = SparkSession.getActiveSession.get
  private var batchReaderOpt: Option[SoapDataSourceBatchReaderV2] = None
  private var streamingReaderOpt: Option[SoapDataSourceStreamingReaderV2] = None

  override def createReader(options: DataSourceOptions): DataSourceReader = {
    createReader(null, options)
  }

  override def createReader(schema: StructType, options: DataSourceOptions): DataSourceReader = {

    batchReaderOpt.map { e =>
      info("Starting batching...")
      e
    }.getOrElse {
      val sparkConf: SparkConf = spark.sparkContext.getConf
      val sfOptions = SfOptions(options.asMap().asScala.toMap, sparkConf)
      val reader = new SoapDataSourceBatchReaderV2(sfOptions, Option(schema))
      batchReaderOpt = Some(reader)
      reader
    }
  }

  // TODO throw an exception if checkpointLocation is not defined and is null
  override def createMicroBatchReader(schema: Optional[StructType], checkpointLocation: String, options: DataSourceOptions): MicroBatchReader = {
    val opt = options
    streamingReaderOpt.map { e =>
      e.checkpointLocation = new Path(checkpointLocation).getParent.getParent.toUri.toString
      info("Starting streaming...")
      e
    }.getOrElse {
      val sparkConf: SparkConf = spark.sparkContext.getConf
      val sfOptions = SfOptions(options.asMap().asScala.toMap, sparkConf)
      val reader = new SoapDataSourceStreamingReaderV2(sfOptions, if (schema.isPresent) Some(schema.get()) else None, checkpointLocation)
      streamingReaderOpt = Some(reader)
      reader
    }
  }
}
