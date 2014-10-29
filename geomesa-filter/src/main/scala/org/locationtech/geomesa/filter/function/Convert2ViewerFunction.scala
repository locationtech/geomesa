/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
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

  import org.locationtech.geomesa.filter.function.Convert2ViewerFunction._

  override def evaluate(obj: scala.Any): String = {
    val id    = getExpression(0).evaluate(obj).asInstanceOf[String]
    val geom  = getExpression(1).evaluate(obj).asInstanceOf[Point]
    val dtg   = dtg2Long(getExpression(2).evaluate(obj))
    Base64.encodeBytes(encode(ExtendedValues(geom.getY.toFloat, geom.getX.toFloat, dtg, None, Some(id))))
  }

  private def dtg2Long(d: Any): Long = d match {
    case l:    Long                           => l
    case jud:  java.util.Date                 => jud.getTime
    case inst: org.joda.time.ReadableInstant  => inst.getMillis
    case inst: org.opengis.temporal.Instant   => inst.getPosition.getDate.getTime
  }
}

object Convert2ViewerFunction {

  /**
   * Simple version:
   *
   * 32-bit int: track_id (a threading key that you can use to color (or not) the dots)
   * 32-bit int: seconds since the epoch
   * 32-bit float: latitude degrees (+/-90)
   * 32-bit float: longitude degrees (+/- 180)
   *
   * Reachback version is the above with an extra
   *
   * 64-bit quantity: label/reachback/whatever you want
   *
   * @param values
   * @return
   */
  def encode(values: EncodedValues): Array[Byte] =
    values match {
      case BasicValues(lat, lon, dtg, trackId) =>
        val buf = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN)
        put(buf, lat, lon, dtg, trackId)
        buf.array()
      case ExtendedValues(lat, lon, dtg, trackId, label) =>
        val buf = ByteBuffer.allocate(24).order(ByteOrder.LITTLE_ENDIAN)
        put(buf, lat, lon, dtg, trackId)
        putOption(buf, label, 8)
        buf.array()
    }

  /**
   * Fills in the basic values
   *
   * @param buffer
   * @param lat
   * @param lon
   * @param dtg
   * @param trackId
   */
  private def put(buffer: ByteBuffer, lat: Float, lon: Float, dtg: Long, trackId: Option[String]): Unit = {
    putOption(buffer, trackId, 4)
    buffer.putInt((dtg / 1000).toInt)
    buffer.putFloat(lat)
    buffer.putFloat(lon)
  }

  /**
   * Puts a string into a fixed size bin, padding with spaces or truncating as needed. If value is
   * 'None', puts 0s for each 4 byte chunk.
   *
   * @param buf
   * @param value
   * @param length number of bytes to use for storing the value
   */
  private def putOption(buf: ByteBuffer, value: Option[String], length: Int): Unit =
    value match {
      case Some(v) =>
        val bytes = v.getBytes
        val sized =
          if (bytes.length < length) {
            bytes.padTo(length, ' '.toByte)
          } else {
            bytes
          }
        buf.put(sized, 0, length)
      case None => buf.position(buf.position + length)
    }

  /**
   * Reads a string from a fixed size bin
   *
   * @param buf
   * @param length number of bytes used to store the value we're reading
   * @return
   */
  private def getOption(buf: ByteBuffer, length: Int): Option[String] = {
    val bytes = Array.ofDim[Byte](length)
    buf.get(bytes)
    if (bytes.forall(_ == 0)) {
      None
    } else {
      Some(new String(bytes).trim)
    }
  }

  /**
   * Decodes a byte array
   *
   * @param encoded
   * @return
   */
  def decode(encoded: Array[Byte]): EncodedValues = {
    val buf = ByteBuffer.wrap(encoded).order(ByteOrder.LITTLE_ENDIAN)
    val trackId = getOption(buf, 4)
    val time = buf.getInt * 1000L
    val lat = buf.getFloat
    val lon = buf.getFloat
    if (encoded.length > 16) {
      val label = getOption(buf, 8)
      ExtendedValues(lat, lon, time, trackId, label)
    } else {
      BasicValues(lat, lon, time, trackId)
    }
  }
}

sealed trait EncodedValues {
  def lat: Float
  def lon: Float
  def dtg: Long
  def trackId: Option[String]
}

case class BasicValues(lat: Float, lon: Float, dtg: Long, trackId: Option[String] = None)
    extends EncodedValues

case class ExtendedValues(lat: Float,
                          lon: Float,
                          dtg: Long,
                          trackId: Option[String] = None,
                          label: Option[String] = None) extends EncodedValues
