package org.apache.spark.ui.streaming.v2

import org.apache.spark.ui.streaming.v2.UIStreamingQueryManager.SF_STREAMING_TAB
import org.apache.spark.ui.{SparkUI, SparkUITab}


case class SfUIStreamingTab(sparkUI: SparkUI) extends SparkUITab(sparkUI, SF_STREAMING_TAB)