/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.locationtech.geomesa.filter.function

import java.time.temporal.{ChronoField, Temporal}
import java.util.Date

import org.locationtech.jts.geom.{Geometry, Point}
import org.geotools.data.Base64
import org.geotools.filter.FunctionExpressionImpl
import org.geotools.filter.capability.FunctionNameImpl
import org.geotools.filter.capability.FunctionNameImpl._
import org.locationtech.geomesa.utils.bin.BinaryEncodeCallback.ByteArrayCallback
import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder
import org.opengis.temporal.Instant

class Convert2ViewerFunction extends FunctionExpressionImpl(Convert2ViewerFunction.Name) {

  override def evaluate(obj: AnyRef): String = {
    val id    = getExpression(0).evaluate(obj).asInstanceOf[String]
    val track = BinaryOutputEncoder.convertToTrack(id)
    val label = BinaryOutputEncoder.convertToLabel(id)
    val geom  = getExpression(1).evaluate(obj).asInstanceOf[Point]
    val dtg   = Convert2ViewerFunction.dtg2Long(getExpression(2).evaluate(obj))
    ByteArrayCallback.apply(track, geom.getY.toFloat, geom.getX.toFloat, dtg, label)
    Base64.encodeBytes(ByteArrayCallback.result)
  }
}

object Convert2ViewerFunction {

  val Name = new FunctionNameImpl(
    "convert2viewer",
    classOf[String],
    parameter("id", classOf[String]),
    parameter("geom", classOf[Geometry]),
    parameter("dtg", classOf[Long])
  )

  private def dtg2Long(dtg: Any): Long = dtg match {
    case d: Long     => d
    case d: Date     => d.getTime
    case d: Temporal => d.getLong(ChronoField.INSTANT_SECONDS) + d.get(ChronoField.MILLI_OF_SECOND)
    case d: Instant  => d.getPosition.getDate.getTime
  }
}
