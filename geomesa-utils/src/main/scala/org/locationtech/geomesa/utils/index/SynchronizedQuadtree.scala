/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.index

import java.util.concurrent.locks.{Lock, ReentrantReadWriteLock}

import org.locationtech.jts.geom.{Envelope, Geometry}
import org.locationtech.jts.index.quadtree.Quadtree

/**
 * Thread safe quad tree
 */
class SynchronizedQuadtree[T] extends SpatialIndex[T] with Serializable {

  import scala.collection.JavaConverters._

  private var qt = new Quadtree

  // quad tree needs to be synchronized - we use a read/write lock which allows concurrent reads but
  // synchronizes writes
  protected [index] val (readLock, writeLock) = {
    val readWriteLock = new ReentrantReadWriteLock()
    (readWriteLock.readLock(), readWriteLock.writeLock())
  }


  override def insert(geom: Geometry, key: String, value: T): Unit =
    withLock(writeLock) { qt.insert(geom.getEnvelopeInternal, (key, value)) }

  override def remove(geom: Geometry, key: String): T = {
    val envelope = geom.getEnvelopeInternal
    val result = withLock(readLock) { qt.query(envelope) }
    result.asScala.asInstanceOf[Seq[(String, T)]].find(_._1 == key) match {
      case None => null.asInstanceOf[T]
      case Some(kv) => withLock(writeLock) { qt.remove(envelope, kv) }; kv._2
    }
  }

  override def get(geom: Geometry, key: String): T = {
    val result = withLock(readLock) { qt.query(geom.getEnvelopeInternal) }
    result.asScala.asInstanceOf[Seq[(String, T)]].find(_._1 == key).map(_._2).getOrElse(null.asInstanceOf[T])
  }

  override def query(xmin: Double, ymin: Double, xmax: Double, ymax: Double): Iterator[T] = {
    val env = new Envelope(xmin, xmax, ymin, ymax)
    val result = withLock(readLock) { qt.query(env) }
    result.iterator.asScala.asInstanceOf[Iterator[(String, T)]].map(_._2)
  }

  override def query(): Iterator[T] = {
    val result = withLock(readLock) { qt.queryAll() }
    result.iterator.asScala.asInstanceOf[Iterator[(String, T)]].map(_._2)
  }

  override def size(): Int = withLock(readLock) { qt.size() }

  override def clear(): Unit = withLock(writeLock) { qt = new Quadtree }

  protected [index] def withLock[V](lock: Lock)(fn: => V): V = {
    lock.lock()
    try { fn } finally { lock.unlock() }
  }
}
