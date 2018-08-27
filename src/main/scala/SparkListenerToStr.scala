
// Note:
// The stages and tasks performance code was written around the basic code of a Spark pipeline:
//
//    https://github.com/apache/spark/blob/master/examples/src/main/scala/org/apache/spark/examples/ml/PipelineExample.scala
//
// although the stages and tasks performance code should be agnostic. It is based on the idea from:
//
//    https://github.com/apache/spark/blob/master/core/src/test/scala/org/apache/spark/scheduler/SparkListenerSuite.scala

import java.lang.System.currentTimeMillis

import scala.collection.mutable

// import org.apache.log4j.{Level, Logger}

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



object SparkListenerToStr {

  // taken from:
  //  https://stackoverflow.com/questions/23128433/simple-iteration-over-case-class-fields
  object Implicits {

    implicit class CaseClassToString(c: AnyRef) {
      def toStringWithFields: String = {
        val fields = (Map[String, Any]() /: c.getClass.getDeclaredFields) { (a, f) =>
          f.setAccessible(true)
          a + (f.getName -> f.get(c))
        }
 
        s"${c.getClass.getName}(${fields.mkString(", ")})"
      }
    }
  }

  import Implicits._

  def convert(event: SparkListenerEvent): String = {

    event match {
      case appEnd: SparkListenerApplicationEnd => {
        appEnd.toStringWithFields
        // s"SparkListenerApplicationEnd: time = ${appEnd.time}"
      }

      case appStart: SparkListenerApplicationStart => {
        s"""SparkListenerApplicationStart: appId: ${appStart.appId}
           |appName: ${appStart.appName}
           |sparkUser: ${appStart.sparkUser}
           |time: ${appStart.time}"""
           .stripMargin.replaceAll("\n", " ")
      }

      // TODO next below
      case _: SparkListenerBlockManagerAdded => {
        "TODO SparkListenerBlockManagerAdded"
      }

      case _: SparkListenerBlockManagerRemoved => {
        "TODO SparkListenerBlockManagerRemoved"
      }

      case _: SparkListenerBlockUpdated => {
        "TODO SparkListenerBlockUpdated"
      }

      case _: SparkListenerEnvironmentUpdate => {
        "TODO SparkListenerEnvironmentUpdate"
      }

      case _: SparkListenerExecutorAdded => {
        "TODO SparkListenerExecutorAdded"
      }

      case _: SparkListenerExecutorBlacklisted => {
        "TODO SparkListenerExecutorBlacklisted"
      }

      case _: SparkListenerExecutorMetricsUpdate => {
        "TODO SparkListenerExecutorMetricsUpdate"
      }

      case _: SparkListenerExecutorRemoved => {
        "TODO SparkListenerExecutorRemoved"
      }

      case _: SparkListenerExecutorUnblacklisted => {
        "TODO SparkListenerExecutorUnblacklisted"
      }

      case _: SparkListenerJobEnd => {
        "TODO SparkListenerJobEnd"
      }

      case _: SparkListenerJobStart => {
        "TODO SparkListenerJobStart"
      }

      case _: SparkListenerLogStart => {
        "TODO SparkListenerLogStart"
      }

      case _: SparkListenerNodeBlacklisted => {
        "TODO SparkListenerNodeBlacklisted"
      }

      case _: SparkListenerNodeUnblacklisted => {
        "TODO SparkListenerNodeUnblacklisted"
      }

      case _: SparkListenerSpeculativeTaskSubmitted => {
        "TODO SparkListenerSpeculativeTaskSubmitted"
      }

      case _: SparkListenerStageCompleted => {
        "TODO SparkListenerStageCompleted"
      }

      case _: SparkListenerStageSubmitted => {
        "TODO SparkListenerStageSubmitted"
      }

      case _: SparkListenerTaskEnd => {
        "TODO SparkListenerTaskEnd"
      }

      case _: SparkListenerTaskGettingResult => {
        "TODO SparkListenerTaskGettingResult"
      }

      case tskStart: SparkListenerTaskStart => {
        tskStart.toStringWithFields
      }

      case _: SparkListenerUnpersistRDD => {
        "TODO SparkListenerUnpersistRDD"
      }

      case _: StreamingQueryListener.QueryProgressEvent => {
        "TODO StreamingQueryListener.QueryProgressEvent"
      }

      case _: StreamingQueryListener.QueryStartedEvent => {
        "TODO StreamingQueryListener.QueryStartedEvent"
      }

      case _: StreamingQueryListener.QueryTerminatedEvent => {
        "TODO StreamingQueryListener.QueryTerminatedEvent"
      }

      case drvAccUpdate: SparkListenerDriverAccumUpdates => {
        s"""SparkListenerDriverAccumUpdates:
                        |executionId: ${drvAccUpdate.executionId}
                        |accumUpdates: ${drvAccUpdate.accumUpdates.mkString("(", ",", ")")}"""
                        .stripMargin.replaceAll("\n", " ")
      }

      case sqlExecEnd: SparkListenerSQLExecutionEnd => {
        s"""SparkListenerSQLExecutionEnd:
                        |executionId: ${sqlExecEnd.executionId}
                        |time: ${sqlExecEnd.time}"""
                        .stripMargin.replaceAll("\n", " ")
      }

      case sqlExecStart: SparkListenerSQLExecutionStart => {
        s"""SparkListenerSQLExecutionStart:
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
                        .stripMargin.replaceAll("\n", " ")
      }

      case otherListenerUnknownType  => {
        s"${otherListenerUnknownType.getClass.getName}: static definition unknown"
      }

    }
  }

}
