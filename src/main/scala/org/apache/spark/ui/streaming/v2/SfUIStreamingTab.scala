package org.apache.spark.ui.streaming.v2

import org.apache.spark.ui.{SparkUI, SparkUITab}
import org.apache.spark.ui.streaming.v2.UIStreamingQueryManager.SF_STREAMING_TAB


case class SfUIStreamingTab(sparkUI: SparkUI) extends SparkUITab(sparkUI, SF_STREAMING_TAB)