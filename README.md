#

Get stages and tasks performance in Apache Spark (tested with Apache Spark 2.3.1)

The underlying example code to demo with performance data was taken from: [https://github.com/apache/spark/blob/master/examples/src/main/scala/org/apache/spark/examples/ml/PipelineExample.scala](https://github.com/apache/spark/blob/master/examples/src/main/scala/org/apache/spark/examples/ml/PipelineExample.scala)

# WIP

This project is a *work in progress*. The implementation is *incomplete* and
subject to change. The documentation can be inaccurate.

# Example Output

(You may change the `log4j.properties` to customize the output.)

        ... DEBUG [spark-listener-group-shared] (AggregateSparkListener.scala:253) - SparkListenerSQLExecutionEnd: executionId: 1 time: 1534130363577
        ... DEBUG [spark-listener-group-shared] (AggregateSparkListener.scala:260) - SparkListenerSQLExecutionStart: executionId: '2' description: 'collect at PipelineExample.scala:102' details: 'org.apache.spark.sql.Dataset.collect(Dataset.scala:2722) PipelineExample$.main(PipelineExample.scala:102) PipelineExample.main(PipelineExample.scala) sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method) sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62) sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43) java.lang.reflect.Method.invoke(Method.java:498) sbt.Run.invokeMain(Run.scala:93) sbt.Run.run0(Run.scala:87) sbt.Run.execute$1(Run.scala:65) sbt.Run.$anonfun$run$4(Run.scala:77) scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.java:12) sbt.util.InterfaceUtil$$anon$1.get(InterfaceUtil.scala:10) sbt.TrapExit$App.run(TrapExit.scala:252) java.lang.Thread.run(Thread.java:745)' physicalPlanDescription: '== Parsed Logical Plan == 'Project [unresolvedalias('id, None), unresolvedalias('text, None), unresolvedalias('probability, None), unresolvedalias('prediction, None)] +- AnalysisBarrier       +- Project [id#117L, text#118, words#123, features#127, rawPrediction#132, probability#138, UDF(rawPrediction#132) AS prediction#145]          +- Project [id#117L, text#118, words#123, features#127, rawPrediction#132, UDF(rawPrediction#132) AS probability#138]             +- Project [id#117L, text#118, words#123, features#127, UDF(features#127) AS rawPrediction#132]                +- Project [id#117L, text#118, words#123, UDF(words#123) AS features#127]                   +- Project [id#117L, text#118, UDF(text#118) AS words#123]                      +- Project [_1#113L AS id#117L, _2#114 AS text#118]                         +- LocalRelation [_1#113L, _2#114]  == Analyzed Logical Plan == id: bigint, text: string, probability: vector, prediction: double Project [id#117L, text#118, probability#138, prediction#145] +- Project [id#117L, text#118, words#123, features#127, rawPrediction#132, probability#138, UDF(rawPrediction#132) AS prediction#145]    +- Project [id#117L, text#118, words#123, features#127, rawPrediction#132, UDF(rawPrediction#132) AS probability#138]       +- Project [id#117L, text#118, words#123, features#127, UDF(features#127) AS rawPrediction#132]          +- Project [id#117L, text#118, words#123, UDF(words#123) AS features#127]             +- Project [id#117L, text#118, UDF(text#118) AS words#123]                +- Project [_1#113L AS id#117L, _2#114 AS text#118]                   +- LocalRelation [_1#113L, _2#114]  == Optimized Logical Plan == LocalRelation [id#117L, text#118, probability#138, prediction#145]  == Physical Plan == LocalTableScan [id#117L, text#118, probability#138, prediction#145]' sparkPlanInfo: { nodeName: LocalTableScan, simpleString: org.apache.spark.sql.execution.SparkPlanInfo@6318164f, children: (), metrics: (org.apache.spark.sql.execution.metric.SQLMetricInfo@11bd244f) } time: 1534130363686
        ... DEBUG [spark-listener-group-shared] (AggregateSparkListener.scala:253) - SparkListenerSQLExecutionEnd: executionId: 2 time: 1534130363727
        (4, spark i j k) --> prob=[0.15964077387874118,0.8403592261212589], prediction=1.0
        (5, l m n) --> prob=[0.8378325685476612,0.16216743145233875], prediction=0.0
        (6, spark hadoop spark) --> prob=[0.06926633132976273,0.9307336686702373], prediction=1.0
        (7, apache hadoop) --> prob=[0.9821575333444208,0.01784246665557917], prediction=0.0
        ... DEBUG [run-main-0] (AggregateSparkListener.scala:346) - StageInfo: id 1 name: treeAggregate at RDDLossFunction.scala:61 parentIds: List()
        ... DEBUG [run-main-0] (AggregateSparkListener.scala:353) -   0:
        ... DEBUG [run-main-0] (AggregateSparkListener.scala:285) - TaskInfo: 7 index: 3 launched at 1534130360204 status: SUCCESS
        ... DEBUG [run-main-0] (AggregateSparkListener.scala:288) -     TaskMetrics:
        ... DEBUG [run-main-0] (AggregateSparkListener.scala:336) - diskBytesSpilled: 0 executorCpuTime: 6846079 executorDeserializeCpuTime: 3667674 executorDeserializeTime: 4 executorRunTime: 13 memoryBytesSpilled: 0 peakExecutionMemory: 0 resultSerializationTime: 0 resultSize: 10554
              InputMetrics: bytesRead 136 recordsRead 1
              OutputMetrics: bytesWritten 0 recordsWritten 0
              ShuffleReadMetrics: fetchWaitTime 0 localBlocksFetched 0 localBytesRead 0 recordsRead 0 remoteBlocksFetched 0 remoteBytesRead 0 remoteBytesReadToDisk 0
              ShuffleWriteMetrics: bytesWritten 0 recordsWritten 0 writeTime 0

