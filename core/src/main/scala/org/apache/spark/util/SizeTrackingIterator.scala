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

package org.apache.spark.util


/**
 * Wrap an iterator to estimate the in-memory size of the iterated-over
 * collection and estimate the in-memory size of the iterated over elements.
 *
 * After hasNext returns false, calls finishCallback with the estimated size in bytes
 * and the number of elements returned.
 */
private[spark] class SizeTrackingIterator[T](
    private val baseIterator: Iterator[T],
    finishCallback: (Long, Long) => Unit) extends Iterator[T] {
  /**
   * Controls the base of the exponential which governs the rate of sampling
   * like SizeTracker. Unlike SizeTracker, our samples our individual elements
   * iterated over, so we sample more frequently.
   */
  private val SAMPLE_GROWTH_RATE = 1.05

  /**
   * Total number of elements sampled.
   */
  private var totalSamples: Long = 0L

  /**
   * Total size of elements sampled.
   */
  private var totalBytesSampled: Long = 0L

  /**
   * Total number of items returned from next()
   */
  private var totalItems: Long = 0L

  /**
   * Next totalItems value to sample at.
   */
  private var nextSampleNum: Long = 1L

  /**
   * True if finished sampling.
   */
  private var finished: Boolean = false

  def currentSizeEstimate: Long =
    math.ceil(totalBytesSampled.toDouble * (totalItems.toDouble / totalSamples)).toLong

  override def hasNext: Boolean = {
    if (finished) {
      false
    } else {
      val result = baseIterator.hasNext
      if (!result) {
        finished = true
        finishCallback(currentSizeEstimate, totalItems)
      }
      result
    }
  }

  private def sampleItem(item: AnyRef) {
    val currentSize = SizeEstimator.estimate(item)
    totalBytesSampled += currentSize
    totalSamples += 1
    nextSampleNum = math.ceil(totalItems * SAMPLE_GROWTH_RATE).toLong
  }

  override def next: T = {
    val result = baseIterator.next
    totalItems += 1
    if (totalItems == nextSampleNum) {
      sampleItem(result.asInstanceOf[AnyRef])
    }
    result
  }
}
