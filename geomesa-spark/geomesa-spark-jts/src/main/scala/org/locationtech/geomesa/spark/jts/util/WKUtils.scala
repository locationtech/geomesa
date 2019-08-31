/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark.jts.util

import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.io._

trait WKTUtils {
  private[this] val readerPool = new ThreadLocal[WKTReader]{
    override def initialValue = new WKTReader
  }
  private[this] val writerPool = new ThreadLocal[WKTWriter]{
    override def initialValue = new WKTWriter(2)
  }
  private[this] val writerPoolZ = new ThreadLocal[WKTWriter]{
    override def initialValue = new WKTWriter(3)
  }
  private[this] val writerPoolZM = new ThreadLocal[WKTWriter]{
    override def initialValue = new WKTWriter(4)
  }

  def read(s: String): Geometry = readerPool.get.read(s)
  def write(g: Geometry): String = {
    val c = g.getCoordinate
    val cz = c.getZ.isNaN
    val cm = c.getM.isNaN
    (cz, cm) match {
      case (true, true) => writerPool.get.write(g)
      case (false, true) => writerPoolZ.get.write(g)
      case (_, false) => writerPoolZM.get.write(g)
    }
  }
}

trait WKBUtils {
  private[this] val readerPool = new ThreadLocal[WKBReader]{
    override def initialValue = new WKBReader
  }
  private[this] val writerPool = new ThreadLocal[WKBWriter]{
    override def initialValue = new WKBWriter(2)
  }
  private[this] val writerPoolZ = new ThreadLocal[WKBWriter]{
    override def initialValue = new WKBWriter(3)
  }

  def read(s: String): Geometry = read(s.getBytes)
  def read(b: Array[Byte]): Geometry = readerPool.get.read(b)
  def write(g: Geometry): Array[Byte] = {
    val c = g.getCoordinate
    val cz = c.getZ.isNaN
    val cm = c.getM.isNaN
    (cz, cm) match {
      case (true, _) => writerPool.get.write(g)
      case (false, _) => writerPoolZ.get.write(g)
    }
  }
}

object WKTUtils extends WKTUtils
object WKBUtils extends WKBUtils
