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

package org.apache.spark.ui.storage

import scala.collection.mutable

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ui._
import org.apache.spark.scheduler._
import org.apache.spark.storage._

/** Web UI showing storage status of all RDD's in the given SparkContext. */
private[ui] class StorageTab(parent: SparkUI) extends SparkUITab(parent, "storage") {
  val listener = parent.storageListener

  attachPage(new StoragePage(this))
  attachPage(new RDDPage(this))
}

/**
 * :: DeveloperApi ::
 * A SparkListener that prepares information to be displayed on the BlockManagerUI.
 */
@DeveloperApi
class StorageListener(storageStatusListener: StorageStatusListener,
                      missRateListener: MissRateListener) extends SparkListener {
  private[ui] val _rddInfoMap = mutable.Map[Int, RDDInfo]() // exposed for testing

  def storageStatusList = storageStatusListener.storageStatusList

  /** Filter RDD info to include only those with cached partitions */
  def rddInfoList = _rddInfoMap.values.filter(_.numCachedPartitions > 0).toSeq

  /** Update the storage info of the RDDs whose blocks are among the given updated blocks */
  private def updateRDDInfo(updatedBlocks: Seq[BlockId]): Unit = {
    val rddIdsToUpdate = updatedBlocks.flatMap { case bid => bid.asRDDId.map(_.rddId) }.toSet
    val rddInfosToUpdate = _rddInfoMap.values.toSeq.filter { s => rddIdsToUpdate.contains(s.id) }
    StorageUtils.updateRddInfo(rddInfosToUpdate, storageStatusList, Some(missRateListener))
  }

  /**
   * Assumes the storage status list is fully up-to-date. This implies the corresponding
   * StorageStatusSparkListener must process the SparkListenerTaskEnd event before this listener.
   */
  override def onTaskEnd(taskEnd: SparkListenerTaskEnd) = synchronized {
    val metrics = taskEnd.taskMetrics
    if (metrics != null && metrics.updatedBlocks.isDefined) {
      updateRDDInfo(metrics.updatedBlocks.get.map(_._1))
    }
    if (metrics != null && metrics.accessedBlocks.isDefined) {
      updateRDDInfo(metrics.accessedBlocks.get.map(_._1))
    }
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted) = synchronized {
    val rddInfos = stageSubmitted.stageInfo.rddInfos
    rddInfos.foreach { info => _rddInfoMap.getOrElseUpdate(info.id, info) }
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted) = synchronized {
    // Remove all partitions that are no longer cached in current completed stage
    val completedRddIds = stageCompleted.stageInfo.rddInfos.map(r => r.id).toSet
    _rddInfoMap.retain { case (id, info) =>
      !completedRddIds.contains(id) || info.numCachedPartitions > 0
    }
  }

  override def onUnpersistRDD(unpersistRDD: SparkListenerUnpersistRDD) = synchronized {
    _rddInfoMap.remove(unpersistRDD.rddId)
  }
}
