/*
 * Copyright 2015 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.utils.index

import java.util.concurrent.locks.{Lock, ReentrantReadWriteLock}

import com.vividsolutions.jts.geom.Envelope
import com.vividsolutions.jts.index.ItemVisitor
import com.vividsolutions.jts.index.quadtree.Quadtree
import scala.collection.JavaConverters._

/**
 * Thread safe quad tree
 */
class SynchronizedQuadtree[T] extends SpatialIndex[T] with Serializable {

  private val qt = new Quadtree

  // quad tree needs to be synchronized - we use a read/write lock which allows concurrent reads but
  // synchronizes writes
  protected[index] val (readLock, writeLock) = {
    val readWriteLock = new ReentrantReadWriteLock()
    (readWriteLock.readLock(), readWriteLock.writeLock())
  }

  override def query(envelope: Envelope) =
    withLock(readLock) { qt.query(envelope).iterator().asScala.asInstanceOf[Iterator[T]] }

  override def insert(envelope: Envelope, item: T) = withLock(writeLock) { qt.insert(envelope, item) }

  override def remove(envelope: Envelope, item: T) = withLock(writeLock) { qt.remove(envelope, item) }

  protected[index] def withLock[T](lock: Lock)(fn: => T) = {
    lock.lock()
    try {
      fn
    } finally {
      lock.unlock()
    }
  }
}
