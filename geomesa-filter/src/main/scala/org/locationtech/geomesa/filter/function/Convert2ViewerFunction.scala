/*
 * Copyright 2013 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.locationtech.geomesa.filter.function

import java.nio.{ByteBuffer, ByteOrder}

import com.vividsolutions.jts.geom.{Geometry, Point}
import org.geotools.data.Base64
import org.geotools.filter.FunctionExpressionImpl
import org.geotools.filter.capability.FunctionNameImpl
import org.geotools.filter.capability.FunctionNameImpl._

class Convert2ViewerFunction
  extends FunctionExpressionImpl(
    new FunctionNameImpl(
      "convert2viewer",
      classOf[String],
      parameter("id", classOf[String]),
      parameter("geom", classOf[Geometry]),
      parameter("dtg", classOf[Long])
    )) {

  override def evaluate(obj: scala.Any) = {
    val id    = getExpression(0).evaluate(obj).asInstanceOf[String]
    val geom  = getExpression(1).evaluate(obj).asInstanceOf[Point]
    val dtg   = dtg2Long(getExpression(2).evaluate(obj))

    val buf = ByteBuffer.allocate(24).order(ByteOrder.LITTLE_ENDIAN)
    buf.putInt(0)
    buf.putInt(dtg.toInt)
    buf.putFloat(geom.getY.toFloat)
    buf.putFloat(geom.getX.toFloat)
    buf.put(id.getBytes.take(8))
    Base64.encodeBytes(buf.array())
  }

  private def dtg2Long(d: Any): Long = d match {
    case l:    Long                           => l
    case jud:  java.util.Date                 => jud.getTime
    case inst: org.joda.time.ReadableInstant  => inst.getMillis
    case inst: org.opengis.temporal.Instant   => inst.getPosition.getDate.getTime
  }

}

