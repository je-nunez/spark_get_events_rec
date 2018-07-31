#

Get stages and tasks performance in Apache Spark (tested with Apache Spark 2.3.1)

The underlying example code to demo with performance data was taken from: [https://github.com/apache/spark/blob/master/examples/src/main/scala/org/apache/spark/examples/ml/PipelineExample.scala](https://github.com/apache/spark/blob/master/examples/src/main/scala/org/apache/spark/examples/ml/PipelineExample.scala)

# WIP

This project is a *work in progress*. The implementation is *incomplete* and
subject to change. The documentation can be inaccurate.

# Example Output

        StageInfo: id 16 name: parquet at LogisticRegression.scala:1233 parentIds: List()
          0: TaskInfo: 52 index: 0 launched at 1533009357617 status: SUCCESS
            TaskMetrics: diskBytesSpilled: 0 executorCpuTime: 26688739 executorDeserializeCpuTime: 2572968 executorDeserializeTime: 3 executorRunTime: 38 memoryBytesSpilled: 0 peakExecutionMemory: 0 resultSerializationTime: 2 resultSize: 1330
              InputMetrics: bytesRead 0 recordsRead 0
              OutputMetrics: bytesWritten 0 recordsWritten 0
              ShuffleReadMetrics: fetchWaitTime 0 localBlocksFetched 0 localBytesRead 0 recordsRead 0 remoteBlocksFetched 0 remoteBytesRead 0 remoteBytesReadToDisk 0
              ShuffleWriteMetrics: bytesWritten 264 recordsWritten 1 writeTime 4363815

