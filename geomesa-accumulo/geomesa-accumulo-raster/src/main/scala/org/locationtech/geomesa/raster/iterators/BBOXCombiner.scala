/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.raster.iterators

import java.util

import org.locationtech.jts.geom.Point
import org.locationtech.jts.io.WKTReader
import org.apache.accumulo.core.data.{Key, Value}
import org.apache.accumulo.core.iterators.Combiner
import org.locationtech.geomesa.raster.iterators.BBOXCombiner._
import org.locationtech.geomesa.utils.geohash.BoundingBox

import scala.collection.JavaConversions._

/*
    The BBOXCombiner is used to reduce known extents into a single Bounding Box
    This is useful for figuring out the total extent of a raster pyramid layer as an example
 */
class BBOXCombiner extends Combiner {
  override def reduce(p1: Key, p2: util.Iterator[Value]): Value = {
    if (p2.hasNext) bboxToValue(reduceValuesToBoundingBox(p2))
    else new Value()
  }
}

object BBOXCombiner {
  val wktReader = new ThreadLocal[WKTReader] {
    override def initialValue = new WKTReader()
  }

  def reduceValuesToBoundingBox(values: util.Iterator[Value]): BoundingBox = {
    values.map(valueToBbox).reduce( (a, b) => BoundingBox.getCoveringBoundingBox(a, b) )
  }

  // These two functions are inverse
  def bboxToValue(bbox: BoundingBox): Value = {
    new Value((bbox.ll.toString + ":" + bbox.ur.toString).getBytes)
  }

  def valueToBbox(value: Value): BoundingBox = {
    val wkts = value.toString.split(":")
    val localReader = wktReader.get
    BoundingBox(localReader.read(wkts(0)).asInstanceOf[Point], localReader.read(wkts(1)).asInstanceOf[Point])
  }
}
