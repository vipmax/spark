// scalastyle:off

package org.apache.spark.streaming.scheduler

import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.cluster.ExecutorInfo
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.receiver.Receiver

import scala.collection.mutable


case class AppStat(time: Long, input: Long, latency: Long, cores: Long)

class DynamicListener(microbatchInterval: Duration, sc: SparkContext) extends SparkListener
  with StreamingListener with Logging {

  val logger = Logger.getLogger(this.getClass.getSimpleName)

  val stats = collection.mutable.ArrayBuffer[AppStat]()

  val executors = collection.mutable.Map[String, ExecutorInfo]()

  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {
    logger.info(s"onExecutorRemoved: $executorRemoved")
    executors -= executorRemoved.executorId
  }

  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
    logger.info(s"onExecutorAdded: $executorAdded")
    executors += executorAdded.executorId -> executorAdded.executorInfo
  }

  /** Called when a batch of jobs has been submitted for processing. */
  override def onBatchSubmitted(batchSubmitted: StreamingListenerBatchSubmitted): Unit = {
    logger.info(s"onBatchSubmitted: $batchSubmitted")
    batchSubmitted.batchInfo.numRecords
  }

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted) {
    logger.info(s"onBatchCompleted: ${batchCompleted.batchInfo.batchTime}")

    val time = batchCompleted.batchInfo.batchTime.milliseconds
    val input = batchCompleted.batchInfo.numRecords
    val latency = batchCompleted.batchInfo.processingDelay.get
    val cores = executors.values.map(_.totalCores).sum.toLong

    logger.info(s"time: $time, input: $input, latency: $latency, cores: $cores")

    stats += AppStat(time, input, latency, cores)

    if(latency > microbatchInterval.milliseconds) {
      logger.info(s"latency: $latency more than microbatchInterval(${microbatchInterval.milliseconds})")
    }

  }
}


class DynamicReceiver() extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) with Logging {

  def onStart() {
    new Thread("Receiver") { override def run() { receive() }}.start()
  }

  def onStop() {}

  private def receive() {
    while (true) {
      store("a")
      Thread.sleep(1000)
    }
  }
}

class PerformanceModel {

  def model(stats: Traversable[AppStat], microbatchInterval: Long) = {
    val avgInput = stats.map(_.input).sum.toDouble / stats.size
    val avgLatency = stats.map(_.latency).sum / stats.size
    val avgCores = stats.map(_.cores).sum / stats.size

    val overheadTime = Math.min(stats.map(_.latency.toInt).min, 10)


    val performance = if (avgLatency <= microbatchInterval) {
      println("Evaluating pmodel for normal latency")

      // performance can be multiply by some number
      val overallPerformance = avgInput * (microbatchInterval / (avgLatency - overheadTime))
      val optimalCores = overallPerformance / avgCores
      optimalCores
    } else {

      val lags = stats.toList.sliding(2).toList.map { case List(o1, o2) => o2.latency - o1.latency }
      val avgLag = lags.sum / lags.size

      if(avgLag > 0) {

        println("Evaluating pmodel for increasing latency")
        // performance should by decreased by some number
        val decreacePercentBase = 0.1
        val decreacePercent = decreacePercentBase  * Math.log10(1 + avgLatency)
        println(s"decreacePercent=$decreacePercent")

        val overallPerformance = avgInput / (1 + decreacePercent)
        val optimalCores = overallPerformance.toDouble / avgCores
        optimalCores
      }
      else { // latency go down
        println("Evaluating pmodel for decreasing latency")
        // nothing to do
        val p = avgInput
        val optimalCores = p / avgCores
        optimalCores
      }
    }

    println(s"PerformanceModel  p=$performance, avgCores=$avgCores, " +
      s"avgInput=$avgInput avgLatency=$avgLatency, overhead_time=$overheadTime")

    performance
  }

}