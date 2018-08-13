
// Note:
// The stages and tasks performance code was written around the basic code of a Spark pipeline:
//
//    https://github.com/apache/spark/blob/master/examples/src/main/scala/org/apache/spark/examples/ml/PipelineExample.scala
//
// although the stages and tasks performance code should be agnostic. It is based on the idea from:
//
//    https://github.com/apache/spark/blob/master/core/src/test/scala/org/apache/spark/scheduler/SparkListenerSuite.scala



import scala.collection.mutable

import org.apache.log4j.{Level, Logger}

import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler.{SparkListener,
                                   SparkListenerApplicationEnd,
                                   SparkListenerApplicationStart,
                                   SparkListenerBlockManagerAdded,
                                   SparkListenerBlockManagerRemoved,
                                   SparkListenerBlockUpdated,
                                   SparkListenerEnvironmentUpdate,
                                   SparkListenerEvent,
                                   SparkListenerExecutorAdded,
                                   SparkListenerExecutorBlacklisted,
                                // SparkListenerExecutorBlacklistedForStage,
                                   SparkListenerExecutorMetricsUpdate,
                                   SparkListenerExecutorRemoved,
                                   SparkListenerExecutorUnblacklisted,
                                   SparkListenerJobEnd,
                                   SparkListenerJobStart,
                                   SparkListenerLogStart,
                                   SparkListenerNodeBlacklisted,
                                // SparkListenerNodeBlacklistedForStage,
                                   SparkListenerNodeUnblacklisted,
                                   SparkListenerSpeculativeTaskSubmitted,
                                   SparkListenerStageCompleted,
                                   SparkListenerStageSubmitted,
                                   SparkListenerTaskEnd,
                                   SparkListenerTaskGettingResult,
                                   StageInfo,
                                   SparkListenerTaskStart,
                                   SparkListenerUnpersistRDD,
                                   TaskInfo}

import org.apache.spark.sql.execution.ui.{SparkListenerDriverAccumUpdates,
                                          SparkListenerSQLExecutionEnd,
                                          SparkListenerSQLExecutionStart}

import org.apache.spark.sql.streaming.StreamingQueryListener

import org.apache.spark.status.api.v1.{InputMetrics,
                                       OutputMetrics,
                                       ShuffleReadMetrics,
                                       ShuffleWriteMetrics}



class AggregateSparkListener extends SparkListener {

  @transient lazy val logger = Logger.getLogger(getClass.getName)

  var taskInfoMetrics = mutable.Buffer[(TaskInfo, TaskMetrics)]()
  val stageInfos = mutable.Map[StageInfo, Seq[(TaskInfo, TaskMetrics)]]()

  override def onTaskEnd(task: SparkListenerTaskEnd) {
    val info = task.taskInfo
    val metrics = task.taskMetrics
    if (info != null && metrics != null) {
      taskInfoMetrics += ((info, metrics))
    }
  }

  override def onStageCompleted(stage: SparkListenerStageCompleted) {
    stageInfos(stage.stageInfo) = taskInfoMetrics
    taskInfoMetrics = mutable.Buffer.empty
  }


  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = { }

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = { }

  override def onTaskGettingResult(taskGettingResult: SparkListenerTaskGettingResult): Unit = { }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = { }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = { }

  override def onEnvironmentUpdate(environmentUpdate: SparkListenerEnvironmentUpdate): Unit = { }

  override def onBlockManagerAdded(blockManagerAdded: SparkListenerBlockManagerAdded): Unit = { }

  override def onBlockManagerRemoved(
      blockManagerRemoved: SparkListenerBlockManagerRemoved): Unit = { }

  override def onUnpersistRDD(unpersistRDD: SparkListenerUnpersistRDD): Unit = { }

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = { }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = { }

  override def onExecutorMetricsUpdate(
      executorMetricsUpdate: SparkListenerExecutorMetricsUpdate): Unit = { }

  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = { }

  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = { }

  override def onExecutorBlacklisted(
      executorBlacklisted: SparkListenerExecutorBlacklisted): Unit = { }

  /*
   *
  override def onExecutorBlacklistedForStage(
      executorBlacklistedForStage: SparkListenerExecutorBlacklistedForStage
    ): Unit = { }

  override def onNodeBlacklistedForStage(
      nodeBlacklistedForStage: SparkListenerNodeBlacklistedForStage
    ): Unit = { }
   *
   */


  override def onExecutorUnblacklisted(
      executorUnblacklisted: SparkListenerExecutorUnblacklisted): Unit = { }

  override def onNodeBlacklisted(
      nodeBlacklisted: SparkListenerNodeBlacklisted): Unit = { }

  override def onNodeUnblacklisted(
      nodeUnblacklisted: SparkListenerNodeUnblacklisted): Unit = { }

  override def onBlockUpdated(blockUpdated: SparkListenerBlockUpdated): Unit = { }

  override def onSpeculativeTaskSubmitted(
      speculativeTask: SparkListenerSpeculativeTaskSubmitted): Unit = { }

  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    event match {
      case appEnd: SparkListenerApplicationEnd => {
        logger.debug(s"SparkListenerApplicationEnd: time = ${appEnd.time}")
      }

      case appStart: SparkListenerApplicationStart => {
        logger.debug(s"""SparkListenerApplicationStart: appId: ${appStart.appId}
                        |appName: ${appStart.appName}
                        |sparkUser: ${appStart.sparkUser}
                        |time: ${appStart.time}"""
                        .stripMargin.replaceAll("\n", " "))
      }

      // TODO next below
      case _: SparkListenerBlockManagerAdded => {
      }

      case _: SparkListenerBlockManagerRemoved => {
      }

      case _: SparkListenerBlockUpdated => {
      }

      case _: SparkListenerEnvironmentUpdate => {
      }

      case _: SparkListenerExecutorAdded => {
      }

      case _: SparkListenerExecutorBlacklisted => {
      }

      case _: SparkListenerExecutorMetricsUpdate => {
      }

      case _: SparkListenerExecutorRemoved => {
      }

      case _: SparkListenerExecutorUnblacklisted => {
      }

      case _: SparkListenerJobEnd => {
      }

      case _: SparkListenerJobStart => {
      }

      case _: SparkListenerLogStart => {
      }

      case _: SparkListenerNodeBlacklisted => {
      }

      case _: SparkListenerNodeUnblacklisted => {
      }

      case _: SparkListenerSpeculativeTaskSubmitted => {
      }

      case _: SparkListenerStageCompleted => {
      }

      case _: SparkListenerStageSubmitted => {
      }

      case _: SparkListenerTaskEnd => {
      }

      case _: SparkListenerTaskGettingResult => {
      }

      case _: SparkListenerTaskStart => {
      }

      case _: SparkListenerUnpersistRDD => {
      }

      case _: StreamingQueryListener.QueryProgressEvent => {
      }

      case _: StreamingQueryListener.QueryStartedEvent => {
      }

      case _: StreamingQueryListener.QueryTerminatedEvent => {
      }

      case drvAccUpdate: SparkListenerDriverAccumUpdates => {
        logger.debug(s"""SparkListenerDriverAccumUpdates:
                        |executionId: ${drvAccUpdate.executionId}
                        |accumUpdates: ${drvAccUpdate.accumUpdates.mkString("(", ",", ")")}"""
                        .stripMargin.replaceAll("\n", " "))
      }

      case sqlExecEnd: SparkListenerSQLExecutionEnd => {
        logger.debug(s"""SparkListenerSQLExecutionEnd:
                        |executionId: ${sqlExecEnd.executionId}
                        |time: ${sqlExecEnd.time}"""
                        .stripMargin.replaceAll("\n", " "))
      }

      case sqlExecStart: SparkListenerSQLExecutionStart => {
        logger.debug(s"""SparkListenerSQLExecutionStart:
                        |executionId: '${sqlExecStart.executionId}'
                        |description: '${sqlExecStart.description}'
                        |details: '${sqlExecStart.details}'
                        |physicalPlanDescription: '${sqlExecStart.physicalPlanDescription}'
                        |sparkPlanInfo: { nodeName: ${sqlExecStart.sparkPlanInfo.nodeName},
                        |simpleString: ${sqlExecStart.sparkPlanInfo},
                        |children: ${sqlExecStart.sparkPlanInfo.children.mkString("(", ",", ")")},
                        |metrics: ${sqlExecStart.sparkPlanInfo.metrics.mkString("(", ",", ")")}
                        |}
                        |time: ${sqlExecStart.time}"""
                        .stripMargin.replaceAll("\n", " "))
      }

      case otherListenerUnknownType  => {
        logger.warn(s"${otherListenerUnknownType.getClass.getName}: static definition unknown")
      }

    }
 }


  def printStats(): Unit = {

    def printTaskMetrics(ti: TaskInfo, tm: TaskMetrics, prefix: String = ""): Unit = {
      logger.debug(s"""TaskInfo: ${ti.taskId} index: ${ti.index}
                      |launched at ${ti.launchTime} status: ${ti.status}"""
                      .stripMargin.replaceAll("\n", " "))
      logger.debug(s"${prefix}${prefix}TaskMetrics: ")

      val sb = new mutable.StringBuilder()
      sb.append(s"""diskBytesSpilled: ${tm.diskBytesSpilled}
                   |executorCpuTime: ${tm.executorCpuTime}
                   |executorDeserializeCpuTime: ${tm.executorDeserializeCpuTime}
                   |executorDeserializeTime: ${tm.executorDeserializeTime}
                   |executorRunTime: ${tm.executorRunTime}
                   |memoryBytesSpilled: ${tm.memoryBytesSpilled}
                   |peakExecutionMemory: ${tm.peakExecutionMemory}
                   |resultSerializationTime: ${tm.resultSerializationTime}
                   |resultSize: ${tm.resultSize}"""
                   .stripMargin.replaceAll("\n", " "))

      //              |inputMetrics: ${tm.inputMetrics}
      //              |outputMetrics: ${tm.outputMetrics}
      //              |jvmGcTime: ${tm.jvmGcTime}

      val im = tm.inputMetrics
      sb.append("\n" + s"""${prefix}${prefix}${prefix}InputMetrics:
                          |bytesRead ${im.bytesRead}
                          |recordsRead ${im.recordsRead}"""
                          .stripMargin.replaceAll("\n", " "))

      val om = tm.outputMetrics
      sb.append("\n" + s"""${prefix}${prefix}${prefix}OutputMetrics:
                          |bytesWritten ${om.bytesWritten}
                          |recordsWritten ${om.recordsWritten}"""
                          .stripMargin.replaceAll("\n", " "))

      val readMetrics = tm.shuffleReadMetrics
      sb.append("\n" + s"""${prefix}${prefix}${prefix}ShuffleReadMetrics:
                          |fetchWaitTime ${readMetrics.fetchWaitTime}
                          |localBlocksFetched ${readMetrics.localBlocksFetched}
                          |localBytesRead ${readMetrics.localBytesRead}
                          |recordsRead ${readMetrics.recordsRead}
                          |remoteBlocksFetched ${readMetrics.remoteBlocksFetched}
                          |remoteBytesRead ${readMetrics.remoteBytesRead}
                          |remoteBytesReadToDisk ${readMetrics.remoteBytesReadToDisk}"""
                          .stripMargin.replaceAll("\n", " "))

      val writeMetrics = tm.shuffleWriteMetrics
      sb.append("\n" + s"""${prefix}${prefix}${prefix}ShuffleWriteMetrics:
                          |bytesWritten ${writeMetrics.bytesWritten}
                          |recordsWritten ${writeMetrics.recordsWritten}
                          |writeTime ${writeMetrics.writeTime}"""
                          .stripMargin.replaceAll("\n", " "))

      logger.debug(sb)
    }

    if (0 < taskInfoMetrics.length) {
      logger.debug("Remaining taskInfoMetrics... ")
      taskInfoMetrics foreach { case (ti, tm) => printTaskMetrics(ti, tm) }
    }

    stageInfos foreach { case (si, seqTiTm) => {

                                 logger.debug(s"""StageInfo: id ${si.stageId}
                                                 |name: ${si.name}
                                                 |parentIds: ${si.parentIds}"""
                                                 .stripMargin.replaceAll("\n", " "))

                                 seqTiTm.zipWithIndex foreach {
                                    case (tuple, idx) => {
                                            logger.debug(s"  $idx: ")
                                            printTaskMetrics(tuple._1, tuple._2, "  ")
                                         }
                                 }
                              }
                       }
  }

}
