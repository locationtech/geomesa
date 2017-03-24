/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.text

import com.vividsolutions.jts.geom.Geometry
import com.vividsolutions.jts.io.{WKBReader, WKBWriter, WKTReader, WKTWriter}
import org.apache.commons.pool2.BasePooledObjectFactory
import org.apache.commons.pool2.impl.{GenericObjectPoolConfig, DefaultPooledObject, GenericObjectPool}

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
    val conf = new GenericObjectPoolConfig
    conf.setMaxTotal(size)
    val pool = new GenericObjectPool[A](new BasePooledObjectFactory[A] {
      def create() = f
      def wrap(a: A) = new DefaultPooledObject[A](a)
    }, conf)
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
  private[this] val writerPool = new ThreadLocal[WKBWriter]{
    override def initialValue = new WKBWriter
  }

  def read(s: String): Geometry = read(s.getBytes)
  def read(b: Array[Byte]): Geometry = readerPool.get.read(b)
  def write(g: Geometry): Array[Byte] = writerPool.get.write(g)
}

object WKTUtils extends WKTUtils
object WKBUtils extends WKBUtils

