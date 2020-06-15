/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.text

import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.io.{WKBReader, WKBWriter, WKTReader, WKTWriter}
import org.apache.commons.pool2.{BasePooledObjectFactory, PooledObject}
import org.apache.commons.pool2.impl.{DefaultPooledObject, GenericObjectPool, GenericObjectPoolConfig}

trait ObjectPoolUtils[A] {
  val pool: GenericObjectPool[A]

  def withResource[B](f: A => B): B = {
    val obj = pool.borrowObject()
    try {
      f(obj)
    } finally {
      pool.returnObject(obj)
    }
  }
}

object ObjectPoolFactory {
  def apply[A](f: => A, size:Int=10): ObjectPoolUtils[A] = new ObjectPoolUtils[A] {
    private val conf = new GenericObjectPoolConfig[A]
    conf.setMaxTotal(size)
    private val factory = new BasePooledObjectFactory[A] {
      override def create(): A = f
      override def wrap(a: A): PooledObject[A] = new DefaultPooledObject[A](a)
    }
    val pool = new GenericObjectPool[A](factory, conf)
  }
}

trait WKTUtils {
  private[this] val readerPool = new ThreadLocal[WKTReader]{
    override def initialValue = new WKTReader
  }
  private[this] val writerPool = new ThreadLocal[WKTWriter]{
    override def initialValue = new WKTWriter
  }

  def read(s: String): Geometry = readerPool.get.read(s)
  def write(g: Geometry): String = writerPool.get.write(g)
}

trait WKBUtils {

  private[this] val readerPool = new ThreadLocal[WKBReader]{
    override def initialValue = new WKBReader
  }

  private[this] val writer2dPool = new ThreadLocal[WKBWriter]{
    override def initialValue = new WKBWriter(2)
  }

  private[this] val writer3dPool = new ThreadLocal[WKBWriter]{
    override def initialValue = new WKBWriter(3)
  }

  def read(s: String): Geometry = read(s.getBytes)
  def read(b: Array[Byte]): Geometry = readerPool.get.read(b)

  def write(g: Geometry): Array[Byte] = {
    val writer = if (is2d(g)) { writer2dPool } else { writer3dPool }
    writer.get.write(g)
  }

  private def is2d(geometry: Geometry): Boolean = {
    // don't trust coord.getDimensions - it always returns 3 in jts
    // instead, check for NaN for the z dimension
    // note that we only check the first coordinate - if a geometry is written with different
    // dimensions in each coordinate, some information may be lost
    if (geometry == null) { true } else {
      val coord = geometry.getCoordinate
      // check for dimensions - use NaN != NaN to verify z coordinate
      // TODO check for M coordinate when added to JTS
      coord == null || java.lang.Double.isNaN(coord.getZ)
    }
  }
}

object WKTUtils extends WKTUtils
object WKBUtils extends WKBUtils

