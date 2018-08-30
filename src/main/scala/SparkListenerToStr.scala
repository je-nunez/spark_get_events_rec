
// Some code in this section is inspired by:
//
//    https://github.com/apache/spark/blob/master/core/src/test/scala/org/apache/spark/scheduler/SparkListenerSuite.scala

import java.lang.System.currentTimeMillis

import scala.collection.mutable

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

  // Inspired by:
  //  https://stackoverflow.com/questions/23128433/simple-iteration-over-case-class-fields
  // with recursive printing of subfields in fields. Another option would be to use a
  // JSON serialization library, to convert the case class not to a string, but to a
  // proper JSON string.

  object Implicits {

    implicit class CaseClassToString(c: AnyRef) {

      // Note: in order to print a case-class extending SparkListenerEvent with recursive descent
      //       into sub-fields, it is important to have a "maxRecursionDepth" limit parameter,
      //       because the graph inside this case class can have circular references
      //       A -> B -> C -> D -> ... -> C, for example, and this would cause a stack overflow.
      //       (Another way to solve this, without a "maxRecursionDepth" parameter, is to keep
      //       track in the accumulator of the list of references that have been visited in the
      //       traversal, to prevent the infinite recursion.)

      def toStringWithFields(maxRecursionDepth: Int): String = {

        val fields = (Map[String, Any]() /: c.getClass.getDeclaredFields) { (a, f) =>
          f.setAccessible(true)
          val fType: Class[_] = f.getType
          val fTypeSuper = fType.getSuperclass
          val fVal: Object = f.get(c)
          if (fVal == null) {
            a + (f.getName -> """null""")
          } else if (fTypeSuper == null || fVal.isInstanceOf[String] ||
                       maxRecursionDepth <= 0) {
            // TODO: handle the case of lists, for probably better representing [pretty-printing]
            //       elements (subfields) in lists (and arrays)
            a + (f.getName -> fVal)
          } else if (fVal.isInstanceOf[AnyRef]) {
                    a + (f.getName + ": " + fType.getName
                             -> f.get(c).toStringWithFields(maxRecursionDepth - 1) )
          } else {
            a + (f.getName -> fVal)
          }
        }

        s"${c.getClass.getName}(${fields.mkString(", ")})"
      }
    }
  }

  import Implicits._

  def convert(sparkEvent: SparkListenerEvent): String = {
    // convert the case class extending SparkListenerEvent up to 3 levels in depth
    sparkEvent.toStringWithFields(3)
  }


  /* Commented out, to have the code below handy if necessary:
   * example of SparkListenerEvent case matching with the different (case) classes extending
   * SparkListenerEvent.
   *
   *
   *  def notUsed(sparkEvent: SparkListenerEvent): String = {
   *
   *    sparkEvent match {
   *      case appEnd: SparkListenerApplicationEnd => {
   *        // s"SparkListenerApplicationEnd: time = ${appEnd.time}"
   *      }
   *
   *      case appStart: SparkListenerApplicationStart => {
   *        s"""SparkListenerApplicationStart: appId: ${appStart.appId}
   *           |appName: ${appStart.appName}
   *           |sparkUser: ${appStart.sparkUser}
   *           |time: ${appStart.time}"""
   *           .stripMargin.replaceAll("\n", " ")
   *      }
   *
   *      // TODO next below
   *      case _: SparkListenerBlockManagerAdded => {
   *        "TODO SparkListenerBlockManagerAdded"
   *      }
   *
   *      case _: SparkListenerBlockManagerRemoved => {
   *        "TODO SparkListenerBlockManagerRemoved"
   *      }
   *
   *      case _: SparkListenerBlockUpdated => {
   *        "TODO SparkListenerBlockUpdated"
   *      }
   *
   *      case _: SparkListenerEnvironmentUpdate => {
   *        "TODO SparkListenerEnvironmentUpdate"
   *      }
   *
   *      case _: SparkListenerExecutorAdded => {
   *        "TODO SparkListenerExecutorAdded"
   *      }
   *
   *      case _: SparkListenerExecutorBlacklisted => {
   *        "TODO SparkListenerExecutorBlacklisted"
   *      }
   *
   *      case _: SparkListenerExecutorMetricsUpdate => {
   *        "TODO SparkListenerExecutorMetricsUpdate"
   *      }
   *
   *      case _: SparkListenerExecutorRemoved => {
   *        "TODO SparkListenerExecutorRemoved"
   *      }
   *
   *      case _: SparkListenerExecutorUnblacklisted => {
   *        "TODO SparkListenerExecutorUnblacklisted"
   *      }
   *
   *      case _: SparkListenerJobEnd => {
   *        "TODO SparkListenerJobEnd"
   *      }
   *
   *      case _: SparkListenerJobStart => {
   *        "TODO SparkListenerJobStart"
   *      }
   *
   *      case _: SparkListenerLogStart => {
   *        "TODO SparkListenerLogStart"
   *      }
   *
   *      case _: SparkListenerNodeBlacklisted => {
   *        "TODO SparkListenerNodeBlacklisted"
   *      }
   *
   *      case _: SparkListenerNodeUnblacklisted => {
   *        "TODO SparkListenerNodeUnblacklisted"
   *      }
   *
   *      case _: SparkListenerSpeculativeTaskSubmitted => {
   *        "TODO SparkListenerSpeculativeTaskSubmitted"
   *      }
   *
   *      case _: SparkListenerStageCompleted => {
   *        "TODO SparkListenerStageCompleted"
   *      }
   *
   *      case _: SparkListenerStageSubmitted => {
   *        "TODO SparkListenerStageSubmitted"
   *      }
   *
   *      case _: SparkListenerTaskEnd => {
   *        "TODO SparkListenerTaskEnd"
   *      }
   *
   *      case _: SparkListenerTaskGettingResult => {
   *        "TODO SparkListenerTaskGettingResult"
   *      }
   *
   *      case tskStart: SparkListenerTaskStart => {
   *        tskStart.toStringWithFields
   *      }
   *
   *      case _: SparkListenerUnpersistRDD => {
   *        "TODO SparkListenerUnpersistRDD"
   *      }
   *
   *      case _: StreamingQueryListener.QueryProgressEvent => {
   *        "TODO StreamingQueryListener.QueryProgressEvent"
   *      }
   *
   *      case _: StreamingQueryListener.QueryStartedEvent => {
   *        "TODO StreamingQueryListener.QueryStartedEvent"
   *      }
   *
   *      case _: StreamingQueryListener.QueryTerminatedEvent => {
   *        "TODO StreamingQueryListener.QueryTerminatedEvent"
   *      }
   *
   *      case drvAccUpdate: SparkListenerDriverAccumUpdates => {
   *        s"""SparkListenerDriverAccumUpdates:
   *                        |executionId: ${drvAccUpdate.executionId}
   *                        |accumUpdates: ${drvAccUpdate.accumUpdates.mkString("(", ",", ")")}"""
   *                        .stripMargin.replaceAll("\n", " ")
   *      }
   *
   *      case sqlExecEnd: SparkListenerSQLExecutionEnd => {
   *        s"""SparkListenerSQLExecutionEnd:
   *                        |executionId: ${sqlExecEnd.executionId}
   *                        |time: ${sqlExecEnd.time}"""
   *                        .stripMargin.replaceAll("\n", " ")
   *      }
   *
   *      case sqlExecStart: SparkListenerSQLExecutionStart => {
   *        s"""SparkListenerSQLExecutionStart:
   *                        |executionId: '${sqlExecStart.executionId}'
   *                        |description: '${sqlExecStart.description}'
   *                        |details: '${sqlExecStart.details}'
   *                        |physicalPlanDescription: '${sqlExecStart.physicalPlanDescription}'
   *                        |sparkPlanInfo: { nodeName: ${sqlExecStart.sparkPlanInfo.nodeName},
   *                        |simpleString: ${sqlExecStart.sparkPlanInfo},
   *                        |children: ${sqlExecStart.sparkPlanInfo.children.mkString("(", ",", ")")},
   *                        |metrics: ${sqlExecStart.sparkPlanInfo.metrics.mkString("(", ",", ")")}
   *                        |}
   *                        |time: ${sqlExecStart.time}"""
   *                        .stripMargin.replaceAll("\n", " ")
   *      }
   *
   *      case otherListenerUnknownType  => {
   *        s"${otherListenerUnknownType.getClass.getName}: static definition unknown"
   *      }
   *
   *    }
   *  }
   *
   */

}
