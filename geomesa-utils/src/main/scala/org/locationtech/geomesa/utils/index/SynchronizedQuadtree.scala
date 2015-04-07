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

/**
 * Thread safe quad tree
 */
class SynchronizedQuadtree extends Quadtree with Serializable {

  // quad tree needs to be synchronized - we use a read/write lock which allows concurrent reads but
  // synchronizes writes
  protected[index] val (readLock, writeLock) = {
    val readWriteLock = new ReentrantReadWriteLock()
    (readWriteLock.readLock(), readWriteLock.writeLock())
  }

  override def query(searchEnv: Envelope) = withLock(readLock) { super.query(searchEnv) }

  override def query(searchEnv: Envelope, visitor: ItemVisitor) =
    withLock(readLock) { super.query(searchEnv, visitor) }

  override def queryAll() = withLock(readLock) { super.queryAll() }

  override def insert(itemEnv: Envelope, item: scala.Any) =
    withLock(writeLock) { super.insert(itemEnv, item) }

  override def remove(itemEnv: Envelope, item: scala.Any) =
    withLock(writeLock) { super.remove(itemEnv, item) }

  override def isEmpty = super.isEmpty

  override def depth() = super.depth()

  override def size() = super.size()

  protected[index] def withLock[T](lock: Lock)(fn: => T) = {
    lock.lock()
    try {
      fn
    } finally {
      lock.unlock()
    }
  }
}
