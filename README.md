#

Get stages and tasks performance in Apache Spark (tested with Apache Spark 2.3.1)

The underlying example code to demo with performance data was taken from: [https://github.com/apache/spark/blob/master/examples/src/main/scala/org/apache/spark/examples/ml/PipelineExample.scala](https://github.com/apache/spark/blob/master/examples/src/main/scala/org/apache/spark/examples/ml/PipelineExample.scala)

# WIP

This project is a *work in progress*. The implementation is *incomplete* and
subject to change. The documentation can be inaccurate.

# EXAMPLE

        StageInfo: id 17 name: parquet at LogisticRegression.scala:1233 parentIds: List(16)
          0: TaskInfo: 17 index: 0 launched at 1532922553469 status: SUCCESS
        TaskMetrics: diskBytesSpilled: 0 executorCpuTime: 352063976 executorDeserializeCpuTime: 11498519 executorDeserializeTime: 11 executorRunTime: 365 memoryBytesSpilled: 0 peakExecutionMemory: 0 resultSerializationTime: 1 resultSize: 2351

