/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

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
