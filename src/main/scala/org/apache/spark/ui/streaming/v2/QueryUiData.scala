package org.apache.spark.ui.streaming.v2

import com.keks.sf.SfOptions
import com.keks.sf.soap.SfSparkPartition
import com.keks.sf.util.DurationPrinter
import com.keks.sf.util.LetterTimeUnit._
import org.apache.spark.ui.streaming.v2.TransformationStatus.TransformationStatus


case class QueryUiData(queryName: String,
                       soql: String,
                       streamingStartTime: Long,
                       lastMicroBatchTime: Long,
                       offsetCol: String,
                       initialOffset: Option[String],
                       maxOffset: Option[String],
                       partitions: Array[SfSparkPartition] = Array.empty,
                       loadedRecords: Int = 0,
                       microBatchesCount: Int = 0,
                       status: TransformationStatus = TransformationStatus.STARTING,
                       queryConf: QueryConf)

object TransformationStatus extends Enumeration {

  type TransformationStatus = Value
  val STARTING = Value
  val PROCESSING_DATA = Value
  val WAITING_DATA = Value
  val SKIPPED = Value
  val FAILED = Value
  val FINISHED = Value

}

class QueryConf(isQueryAll: Boolean,
                isSelectAll: Boolean,
                apiVersion: String,
                proxyHostOpt: Option[String],
                proxyPortOpt: Option[Int],
                proxyUserNameOpt: Option[String],
                proxyPasswordDefined: Option[String],
                sfUserName: String,
                authEndPoint: String,
                useHttps: Boolean,
                maxSfBatchesPerPartition: Int,
                requestWaitTimeForNewData: Int,
                incrementalLoading: Boolean) {

  override def toString =
    s"""isQueryAll: $isQueryAll;
       |isSelectAll: $isSelectAll;
       |apiVersion: $apiVersion;
       |proxyHostOpt: ${proxyHostOpt.getOrElse("Not defined")};
       |proxyPortOpt: ${proxyPortOpt.getOrElse("Not defined")};
       |proxyUserNameOpt: ${proxyUserNameOpt.getOrElse("Not defined")};
       |proxyPasswordDefined: ${proxyPasswordDefined.map(_ => "Defined").getOrElse("Not defined")};
       |sfUserName: $sfUserName;
       |authEndPoint: $authEndPoint;
       |useHttps: $useHttps;
       |maxSfBatchesPerPartition: $maxSfBatchesPerPartition;
       |requestWaitTimeForNewData: ${DurationPrinter.print[H, M, S, MS](requestWaitTimeForNewData)};
       |incrementalLoading: $incrementalLoading;
       |""".stripMargin

}

object QueryConf {

  def apply(sfOptions: SfOptions): QueryConf = {
    new QueryConf(
      isQueryAll = sfOptions.isQueryAll,
      isSelectAll = sfOptions.isSelectAll,
      apiVersion = sfOptions.apiVersion,
      proxyHostOpt = sfOptions.proxyHostOpt,
      proxyPortOpt = sfOptions.proxyPortOpt,
      proxyUserNameOpt = sfOptions.proxyUserNameOpt,
      proxyPasswordDefined = sfOptions.proxyPasswordOpt,
      sfUserName = sfOptions.userName,
      authEndPoint = sfOptions.authEndPoint,
      useHttps = sfOptions.useHttps,
      maxSfBatchesPerPartition = sfOptions.streamingMaxBatches,
      requestWaitTimeForNewData = sfOptions.streamingAdditionalWaitWhenIncrementalLoading,
      incrementalLoading = !sfOptions.streamingLoadAvailableData)
  }

}