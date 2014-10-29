/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.core.util

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{Executors, TimeUnit}

import com.google.common.collect.Queues
import com.typesafe.scalalogging.slf4j.Logging
import org.apache.accumulo.core.client.{BatchScanner, Scanner}
import org.apache.accumulo.core.data.{Key, Value, Range => AccRange}

import scala.collection.JavaConversions._

class BatchMultiScanner(in: Scanner,
                        out: BatchScanner,
                        joinFn: java.util.Map.Entry[Key, Value] => AccRange,
                        batchSize: Int = 32768)
  extends Iterable[java.util.Map.Entry[Key, Value]] with AutoCloseable with Logging {

  if(batchSize < 1) {
    throw new IllegalArgumentException(f"Illegal batchSize($batchSize%d). Value must be > 0")
  }
  logger.trace(f"Creating BatchMultiScanner with batchSize $batchSize%d")

  type KVEntry = java.util.Map.Entry[Key, Value]
  val inExecutor  = Executors.newSingleThreadExecutor()
  val outExecutor = Executors.newSingleThreadExecutor()
  val inQ  = Queues.newLinkedBlockingQueue[KVEntry](batchSize)
  val outQ = Queues.newArrayBlockingQueue[KVEntry](batchSize)
  val inDone  = new AtomicBoolean(false)
  val outDone = new AtomicBoolean(false)

  inExecutor.submit(new Runnable {
    override def run(): Unit = {
      try {
        in.iterator().foreach(inQ.put)
      } finally {
        inDone.set(true)
      }
    }
  })

  def mightHaveAnother = !inDone.get || !inQ.isEmpty

  outExecutor.submit(new Runnable {
    override def run(): Unit = {
      try {
        while (mightHaveAnother) {
          val entry = inQ.poll(5, TimeUnit.MILLISECONDS)
          if (entry != null) {
            val entries = new collection.mutable.ListBuffer[KVEntry]()
            inQ.drainTo(entries)
            val ranges = (List(entry) ++ entries).map(joinFn)
            out.setRanges(ranges)
            out.iterator().foreach(outQ.put)
          }
        }
      } catch {
        case _: InterruptedException =>
      } finally {
        outDone.set(true)
      }
    }
  })

  override def close() {
    if (!inExecutor.isShutdown) inExecutor.shutdownNow()
    if (!outExecutor.isShutdown) outExecutor.shutdownNow()
    in.close()
    out.close()
  }

  override def iterator: Iterator[KVEntry] = new Iterator[KVEntry] {

    var prefetch: KVEntry = null

    // Indicate there MAY be one more in the outQ but not for sure
    def mightHaveAnother = !outDone.get || !outQ.isEmpty

    def prefetchIfNull() = {
      if (prefetch == null) {
        // loop while we might have another and we haven't set prefetch
        while (mightHaveAnother && prefetch == null) {
          prefetch = outQ.poll
        }
      }
    }

    // must attempt a prefetch since we don't know whether or not the outQ
    // will actually be filled with an item (filters may not match and the
    // in scanner may never return a range)
    override def hasNext(): Boolean = {
      prefetchIfNull()
      prefetch != null
    }

    override def next(): KVEntry = {
      prefetchIfNull()

      val ret = prefetch
      prefetch = null
      ret
    }
  }
}
