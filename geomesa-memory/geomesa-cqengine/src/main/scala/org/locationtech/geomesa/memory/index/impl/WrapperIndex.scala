/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.memory.index.impl

import org.locationtech.geomesa.memory.index.SpatialIndex
import org.locationtech.jts.geom.{Envelope, Geometry}
import org.locationtech.jts.index.{SpatialIndex => JSTSpatialIndex}

import scala.collection.JavaConverters._

class WrapperIndex[T, Index <: JSTSpatialIndex](val indexBuilder: () => Index) extends SpatialIndex[T] with Serializable {

  protected var index: Index = indexBuilder()

  override def insert(geom: Geometry, key: String, value: T): Unit = index.insert(geom.getEnvelopeInternal, (key, value))

  override def remove(geom: Geometry, key: String): T = {
    val envelope = geom.getEnvelopeInternal
    index.query(envelope).asScala.asInstanceOf[Seq[(String, T)]].find(_._1 == key) match {
      case None => null.asInstanceOf[T]
      case Some(kv) => index.remove(envelope, kv); kv._2
    }
  }

  override def get(geom: Geometry, key: String): T = {
    val intersect = index.query(geom.getEnvelopeInternal).asScala.asInstanceOf[Seq[(String, T)]]
    intersect.find(_._1 == key).map(_._2).getOrElse(null.asInstanceOf[T])
  }

  override def query(xmin: Double, ymin: Double, xmax: Double, ymax: Double): Iterator[T] =
    index.query(new Envelope(xmin, xmax, ymin, ymax)).iterator.asScala.asInstanceOf[Iterator[(String, T)]].map(_._2)

  override def query(): Iterator[T] = query(-180,-90,180,90)

  override def size(): Int = query().size

  override def clear(): Unit = index = indexBuilder()
}


