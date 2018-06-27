/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.index

import com.vividsolutions.jts.geom.Envelope
import com.vividsolutions.jts.index.quadtree.Quadtree

import scala.collection.JavaConverters._

/**
 * Spatial index wrapper for un-synchronized quad tree
 */
class WrappedQuadtree[T] extends SpatialIndex[T] with Serializable {

  private var qt = new Quadtree

  override def insert(x: Double, y: Double, key: String, item: T): Unit = insert(new Envelope(x, x, y, y), key, item)

  override def insert(envelope: Envelope, key: String, item: T): Unit = qt.insert(envelope, (key, item))

  override def remove(x: Double, y: Double, key: String): T = remove(new Envelope(x, x, y, y), key)

  override def remove(envelope: Envelope, key: String): T = {
    qt.query(envelope).asScala.asInstanceOf[Seq[(String, T)]].find(_._1 == key) match {
      case None => null.asInstanceOf[T]
      case Some(kv) => qt.remove(envelope, kv); kv._2
    }
  }

  override def get(x: Double, y: Double, key: String): T = get(new Envelope(x, x, y, y), key)

  override def get(envelope: Envelope, key: String): T =
    qt.query(envelope).asScala.asInstanceOf[Seq[(String, T)]].find(_._1 == key).map(_._2).getOrElse(null.asInstanceOf[T])

  override def query(xmin: Double, ymin: Double, xmax: Double, ymax: Double): Iterator[T] =
    qt.query(new Envelope(xmin, xmax, ymin, ymax)).iterator.asScala.asInstanceOf[Iterator[(String, T)]].map(_._2)

  override def query(): Iterator[T] =
    qt.queryAll().iterator.asScala.asInstanceOf[Iterator[(String, T)]].map(_._2)

  override def size(): Int = qt.size()

  override def clear(): Unit = qt = new Quadtree
}
