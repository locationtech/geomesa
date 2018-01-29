/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.locationtech.geomesa.filter.function

import java.time.temporal.ChronoField

import com.vividsolutions.jts.geom.{Geometry, Point}
import org.geotools.data.Base64
import org.geotools.filter.FunctionExpressionImpl
import org.geotools.filter.capability.FunctionNameImpl
import org.geotools.filter.capability.FunctionNameImpl._
import org.locationtech.geomesa.utils.bin.BinaryEncodeCallback.ByteArrayCallback
import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder

class Convert2ViewerFunction
  extends FunctionExpressionImpl(
    new FunctionNameImpl(
      "convert2viewer",
      classOf[String],
      parameter("id", classOf[String]),
      parameter("geom", classOf[Geometry]),
      parameter("dtg", classOf[Long])
    )) {

  override def evaluate(obj: Any): String = {
    val id    = getExpression(0).evaluate(obj).asInstanceOf[String]
    val track = BinaryOutputEncoder.convertToTrack(id)
    val label = BinaryOutputEncoder.convertToLabel(id)
    val geom  = getExpression(1).evaluate(obj).asInstanceOf[Point]
    val dtg   = dtg2Long(getExpression(2).evaluate(obj))
    ByteArrayCallback.apply(track, geom.getY.toFloat, geom.getX.toFloat, dtg, label)
    Base64.encodeBytes(ByteArrayCallback.result)
  }

  private def dtg2Long(d: Any): Long = d match {
    case l:    Long                         => l
    case jud:  java.util.Date               => jud.getTime
    case tmp: java.time.temporal.Temporal   => tmp.getLong(ChronoField.INSTANT_SECONDS) + tmp.get(ChronoField.MILLI_OF_SECOND)
    case inst: org.opengis.temporal.Instant => inst.getPosition.getDate.getTime
  }
}
