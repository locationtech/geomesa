/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark.jts.util

import com.vividsolutions.jts.geom.Geometry
import com.vividsolutions.jts.io._
import org.locationtech.geomesa.jts.GeoMesaWKBReader
import org.locationtech.geomesa.spark.jts.util.WKBUtils.WKBData

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

trait JTSWKBUtils {
  private[this] val readerPool = new ThreadLocal[WKBReader]{
    override def initialValue = new WKBReader
  }
  private[this] val writerPool = new ThreadLocal[WKBWriter]{
    override def initialValue = new WKBWriter
  }

  def read(s: String): Geometry = read(s.getBytes)
  def read(b: Array[Byte]): Geometry = readerPool.get.read(b)
  def write(g: Geometry): WKBData = writerPool.get.write(g).asInstanceOf[WKBData]
}

trait GMWKBUtils {
  private[this] val readerPool = new ThreadLocal[GeoMesaWKBReader]{
    override def initialValue = new GeoMesaWKBReader
  }
  private[this] val writerPool = new ThreadLocal[WKBWriter]{
    override def initialValue = new WKBWriter
  }

  def read(s: String): Geometry = read(s.getBytes)
  def read(b: Array[Byte]): Geometry = readerPool.get.read(b)
  def write(g: Geometry): WKBData = writerPool.get.write(g).asInstanceOf[WKBData]
}

object WKTUtils extends WKTUtils

object GMWKBUtils extends GMWKBUtils {
  trait RawWKB
  type WKBData = Array[Byte] with RawWKB
}

object WKBUtils extends JTSWKBUtils {
  trait RawWKB
  type WKBData = Array[Byte] with RawWKB
}
