/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.index

import org.locationtech.jts.geom.{Envelope, Geometry}
import org.locationtech.jts.index.quadtree.Quadtree

import scala.collection.JavaConverters._

/**
 * Spatial index wrapper for un-synchronized quad tree
 */
class WrappedQuadtree[T] extends SpatialIndex[T] with Serializable {

  private var qt = new Quadtree


  override def insert(geom: Geometry, key: String, value: T): Unit = qt.insert(geom.getEnvelopeInternal, (key, value))

  override def remove(geom: Geometry, key: String): T = {
    val envelope = geom.getEnvelopeInternal
    qt.query(envelope).asScala.asInstanceOf[Seq[(String, T)]].find(_._1 == key) match {
      case None => null.asInstanceOf[T]
      case Some(kv) => qt.remove(envelope, kv); kv._2
    }
  }

  override def get(geom: Geometry, key: String): T = {
    val intersect = qt.query(geom.getEnvelopeInternal).asScala.asInstanceOf[Seq[(String, T)]]
    intersect.find(_._1 == key).map(_._2).getOrElse(null.asInstanceOf[T])
  }

  override def query(xmin: Double, ymin: Double, xmax: Double, ymax: Double): Iterator[T] =
    qt.query(new Envelope(xmin, xmax, ymin, ymax)).iterator.asScala.asInstanceOf[Iterator[(String, T)]].map(_._2)

  override def query(): Iterator[T] =
    qt.queryAll().iterator.asScala.asInstanceOf[Iterator[(String, T)]].map(_._2)

  override def size(): Int = qt.size()

  override def clear(): Unit = qt = new Quadtree
}
