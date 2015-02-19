/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.executor

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkEnv
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.storage.{BlockId, BlockStatus, ShuffleBlockId}

/**
 * :: DeveloperApi ::
 * Metrics tracked during the execution of a task.
 *
 * This class is used to house metrics both for in-progress and completed tasks. In executors,
 * both the task thread and the heartbeat thread write to the TaskMetrics. The heartbeat thread
 * reads it to send in-progress metrics, and the task thread reads it to send metrics along with
 * the completed task.
 *
 * So, when adding new fields, take into consideration that the whole object can be serialized for
 * shipping off at any time to consumers of the SparkListener interface.
 */
@DeveloperApi
class TaskMetrics extends Serializable {
  /**
   * Host's name the task runs on
   */
  var hostname: String = _

  /**
   * Time taken on the executor to deserialize this task
   */
  var executorDeserializeTime: Long = _

  /**
   * Time the executor spends actually running the task (including fetching shuffle data)
   */
  var executorRunTime: Long = _

  /**
   * The number of bytes this task transmitted back to the driver as the TaskResult
   */
  var resultSize: Long = _

  /**
   * Amount of time the JVM spent in garbage collection while executing this task
   */
  var jvmGCTime: Long = _

  /**
   * Amount of time spent serializing the task result
   */
  var resultSerializationTime: Long = _

  /**
   * The number of in-memory bytes spilled by this task
   */
  var memoryBytesSpilled: Long = _

  /**
   * The number of on-disk bytes spilled by this task
   */
  var diskBytesSpilled: Long = _

  /**
   * If this task reads from a HadoopRDD or from persisted data, metrics on how much data was read
   * are stored here.
   */
  var inputMetrics: Option[InputMetrics] = None

  /**
   * If this task writes data externally (e.g. to a distributed filesystem), metrics on how much
   * data was written are stored here.
   */
  var outputMetrics: Option[OutputMetrics] = None

  /**
   * If this task reads from shuffle output, metrics on getting shuffle data will be collected here.
   * This includes read metrics aggregated over all the task's shuffle dependencies.
   */
  private var _shuffleReadMetrics: Option[ShuffleReadMetrics] = None

  def shuffleReadMetrics = _shuffleReadMetrics

  /**
   * This should only be used when recreating TaskMetrics, not when updating read metrics in
   * executors.
   */
  private[spark] def setShuffleReadMetrics(shuffleReadMetrics: Option[ShuffleReadMetrics]) {
    _shuffleReadMetrics = shuffleReadMetrics
  }

  /**
   * ShuffleReadMetrics per dependency for collecting independently while task is in progress.
   */
  @transient private lazy val depsShuffleReadMetrics: ArrayBuffer[ShuffleReadMetrics] =
    new ArrayBuffer[ShuffleReadMetrics]()

  /**
   * If this task writes to shuffle output, metrics on the written shuffle data will be collected
   * here
   */
  var shuffleWriteMetrics: Option[ShuffleWriteMetrics] = None

  /**
   * Storage statuses of any blocks that have been updated as a result of this task.
   */
  var updatedBlocks: Option[Seq[(BlockId, BlockStatus)]] = None

  /**
   * Estimated size of in-memory data returned from Aggregator iterator.
   */
  var shuffleMemoryMetrics: Option[ShuffleMemoryMetrics] = None

  /**
   * Add to output in-memory size metrics.
   */
  private[spark] def incrementMemoryMetrics(bytes: Long, groups: Long) {
    val memoryMetrics = shuffleMemoryMetrics.getOrElse(new ShuffleMemoryMetrics)
    memoryMetrics.shuffleOutputGroups += groups
    memoryMetrics.shuffleOutputBytes += bytes
    shuffleMemoryMetrics = Some(memoryMetrics)
  }

  /**
   * Records of each attempted explicit block access and its result, in chronological order.
   *
   * This record should not include blocks that are not accessed directly by this task, for
   * example blocks which are evicted to disk because this task stores a block.
   *
   * Shuffles will be recorded here using ShuffleBlockId() and map ID 0 for reads and
   * reduce ID 0 for writes. Such records stand in for the entire shuffle being read/written.
   *
   * RDD block ID accesses should always have InputMetrics recorded with them if they did
   * not require recomputation.
   */
  var accessedBlocks: Option[Seq[(BlockId, BlockAccess)]] = None

  private[spark] def recordBlockAccess(blockId: BlockId, blockAccess: BlockAccess) = {
     val oldBlocksAccessed = accessedBlocks.getOrElse(Seq[(BlockId, BlockAccess)]())
     accessedBlocks = Some(oldBlocksAccessed ++ Seq(blockId -> blockAccess))
  }

  /**
   * Records (shuffle ID, map ID) of shuffles written by this task.
   */
  private[spark] def recordWriteShuffle(shuffleId: Int, mapId: Int) {
    recordBlockAccess(ShuffleBlockId(shuffleId, mapId, 0), BlockAccess(BlockAccessType.Write))
  }

  /**
   * Records (shuffle ID, start partition ID, end partition ID) of shuffles read by this task.
   */
  private[spark] def recordReadShuffle(shuffleId: Int, startPartition: Int, endPartition: Int) {
    for (partition <- startPartition to endPartition) {
      recordBlockAccess(ShuffleBlockId(shuffleId, 0, partition), BlockAccess(BlockAccessType.Read))
    }
  }

  /**
   * A task may have multiple shuffle readers for multiple dependencies. To avoid synchronization
   * issues from readers in different threads, in-progress tasks use a ShuffleReadMetrics for each
   * dependency, and merge these metrics before reporting them to the driver. This method returns
   * a ShuffleReadMetrics for a dependency and registers it for merging later.
   */
  private [spark] def createShuffleReadMetricsForDependency(): ShuffleReadMetrics = synchronized {
    val readMetrics = new ShuffleReadMetrics()
    depsShuffleReadMetrics += readMetrics
    readMetrics
  }

  /**
   * Aggregates shuffle read metrics for all registered dependencies into shuffleReadMetrics.
   */
  private[spark] def updateShuffleReadMetrics() = synchronized {
    val merged = new ShuffleReadMetrics()
    for (depMetrics <- depsShuffleReadMetrics) {
      merged.fetchWaitTime += depMetrics.fetchWaitTime
      merged.localBlocksFetched += depMetrics.localBlocksFetched
      merged.remoteBlocksFetched += depMetrics.remoteBlocksFetched
      merged.remoteBytesRead += depMetrics.remoteBytesRead
    }
    _shuffleReadMetrics = Some(merged)
  }
}

private[spark] object TaskMetrics {
  def empty: TaskMetrics = new TaskMetrics

  def extraMetricsEnabled: Boolean = {
    return SparkEnv.get.conf.getBoolean("spark.extraMetrics.enabled", false)
  }

  def ifExtraMetrics(func: => Unit): Unit = {
    if (extraMetricsEnabled) {
      func
    }
  }
}

/**
 * :: DeveloperApi ::
 * Method by which input data was read.  Network means that the data was read over the network
 * from a remote block manager (which may have stored the data on-disk or in-memory).
 *
 * Unavailable indicates that this is a record of a read miss (block was subsequently
 * recomputed), which only occur in blocksAccessed records.
 */
@DeveloperApi
object DataReadMethod extends Enumeration with Serializable {
  type DataReadMethod = Value
  val Memory, Disk, Hadoop, Network, Unavailable = Value
}

/**
 * :: DeveloperApi ::
 * Method by which output data was written.
 */
@DeveloperApi
object DataWriteMethod extends Enumeration with Serializable {
  type DataWriteMethod = Value
  val Hadoop = Value
}

/**
 * :: DeveloperApi ::
 * Metrics about reading input data.
 */
@DeveloperApi
case class InputMetrics(readMethod: DataReadMethod.Value) {
  /**
   * Total bytes read.
   */
  var bytesRead: Long = 0L
}

/**
 * :: DeveloperApi ::
 * Metrics about writing output data.
 */
@DeveloperApi
case class OutputMetrics(writeMethod: DataWriteMethod.Value) {
  /**
   * Total bytes written
   */
  var bytesWritten: Long = 0L
}

/**
 * :: DeveloperApi ::
 * Type of block access made by the task.
 */
@DeveloperApi
object BlockAccessType extends Enumeration with Serializable {
  type BlockAccessType = Value
  val Read, Write = Value
}

/**
 * :: DeveloperApi ::
 * Record of a block access (read or write). For reads, includes metrics on how much was read
 * or None if the block was not found.
 */
@DeveloperApi
case class BlockAccess(accessType: BlockAccessType.Value,
                       inputMetrics: Option[InputMetrics] = None) { 
}

/**
 * :: DeveloperApi ::
 * Metrics pertaining to shuffle data read in a given task.
 */
@DeveloperApi
class ShuffleReadMetrics extends Serializable {
  /**
   * Number of blocks fetched in this shuffle by this task (remote or local)
   */
  def totalBlocksFetched: Int = remoteBlocksFetched + localBlocksFetched

  /**
   * Number of remote blocks fetched in this shuffle by this task
   */
  var remoteBlocksFetched: Int = _

  /**
   * Number of local blocks fetched in this shuffle by this task
   */
  var localBlocksFetched: Int = _

  /**
   * Time the task spent waiting for remote shuffle blocks. This only includes the time
   * blocking on shuffle input data. For instance if block B is being fetched while the task is
   * still not finished processing block A, it is not considered to be blocking on block B.
   */
  var fetchWaitTime: Long = _

  /**
   * Total number of remote bytes read from the shuffle by this task
   */
  var remoteBytesRead: Long = _
}

/**
 * :: DeveloperApi ::
 * Metrics pertaining to shuffle data read in a given task.
 */
@DeveloperApi
class ShuffleMemoryMetrics extends Serializable {
  /**
   * Number of groups returned by Aggregators in this task.
   */
  var shuffleOutputGroups: Long = 0L

  /**
   * Estimated in-memory size in bytes returned by Aggregators in this task.
   */
  var shuffleOutputBytes: Long = 0L
}

/**
 * :: DeveloperApi ::
 * Metrics pertaining to shuffle data written in a given task.
 */
@DeveloperApi
class ShuffleWriteMetrics extends Serializable {
  /**
   * Number of bytes written for the shuffle by this task
   */
  @volatile var shuffleBytesWritten: Long = _

  /**
   * Time the task spent blocking on writes to disk or buffer cache, in nanoseconds
   */
  @volatile var shuffleWriteTime: Long = _
}
