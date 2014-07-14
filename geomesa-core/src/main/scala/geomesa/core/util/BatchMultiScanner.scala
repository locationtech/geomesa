/*
 * Copyright 2013 Commonwealth Computer Research, Inc.
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

package geomesa.core.util

import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean

import com.google.common.collect.Queues
import com.typesafe.scalalogging.slf4j.Logging
import org.apache.accumulo.core.client.{BatchScanner, Scanner}
import org.apache.accumulo.core.data.{Key, Value, Range => AccRange}

import scala.collection.JavaConversions._

// Unused for now.
class BatchMultiScanner(in: Scanner,
                        out: BatchScanner,
                        joinFn: java.util.Map.Entry[Key, Value] => AccRange)
  extends Iterable[java.util.Map.Entry[Key, Value]] with Logging {

  type E = java.util.Map.Entry[Key, Value]
  val inExecutor  = Executors.newSingleThreadExecutor()
  val outExecutor = Executors.newSingleThreadExecutor()
  val inQ  = Queues.newLinkedBlockingQueue[E](32768)
  val outQ = Queues.newArrayBlockingQueue[E](32768)
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

  def moreInQ = !(inDone.get && inQ.isEmpty)

  outExecutor.submit(new Runnable {
    override def run(): Unit = {
      try {
        while(moreInQ) {
          val entry = inQ.take()
          if(entry != null) {
            val entries = new collection.mutable.ListBuffer[E]()
            val count = inQ.drainTo(entries)
            if (count > 0) {
              val ranges = (List(entry) ++ entries).map(joinFn)
              out.setRanges(ranges)
              out.iterator().foreach(e => outQ.put(e))
            }
          }
        }
        outDone.set(true)
      } catch {
        case _: InterruptedException =>
      } finally {
        outDone.set(true)
      }
    }
  })

  override def iterator: Iterator[java.util.Map.Entry[Key, Value]] = new Iterator[E] {
    override def hasNext: Boolean = {
      val ret = !(outQ.isEmpty && inDone.get() && outDone.get())
      if(!ret) {
        inExecutor.shutdownNow()
        outExecutor.shutdownNow()
      }
      ret
    }

    override def next(): E = outQ.take()
  }
}
