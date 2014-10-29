/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.core.iterators

import java.nio.ByteBuffer

import org.apache.accumulo.core.data.{Key, Value}
import org.locationtech.geomesa.utils.geohash.{BoundingBox, Bounds, GeoHash}


class SurfaceAggregatingIterator extends KeyAggregator {
  var bbox:BoundingBox = null
  var xdim = 0
  var ydim= 0
  var blHash:String = null
  var trHash:String = null
  var buffer:Array[Byte] = null
  var dxDegrees = 0.0
  var dyDegrees = 0.0
  var precision = 0
  var normalizationConstant= 255

  var aggOpt = AggregatingKeyIterator.aggOpt

  def reset() {
    buffer = createImageBuffer(xdim,ydim)
  }

  def collect(key: Key, value: Value) {

    val gh = GeoHash(key.getRow.toString, precision)
    if (bbox.intersects(gh.bbox)// @todo faster?
    ){
      val x: Int = Math.min( Math.max( Math.round(xdim * (gh.x - bbox.ll.getX)/ dxDegrees)
                   .asInstanceOf[Int], 0), xdim-1)
      // subtract from ydim - images count from top left, geohash from bottom left
      val y: Int = ydim -1- Math.min(Math.max(Math.round(ydim * (gh.y - bbox.ll.getY)/ dyDegrees)
                            .asInstanceOf[Int], 0), ydim - 1)
      val idx: Int = y * xdim + x
      val thisVal: Double = ByteBuffer.wrap(value.get()).getDouble
      //since Byte is signed, do some bit maths
      buffer(idx) = math.max((buffer(idx)&0xff), (thisVal * normalizationConstant).toInt).asInstanceOf[Byte]
    }
  }

  def aggregate = new Value(buffer)

  def setOpt(opt: String, value: String) {
    opt.substring(aggOpt.length) match{
      case "bottomLeft" => blHash = value
      case "topRight" => trHash = value
      case "precision" => precision = value.toInt
      case "dims" => {
        val dims = value.split(",")
        xdim = dims(0).toInt
        ydim = dims(1).toInt
      }
      case _ => throw new Exception("not found!" + opt)
    }
    if (blHash != null && trHash != null && precision > 0 && xdim >0 && ydim > 0) {
      buffer =createImageBuffer(xdim, ydim)
      val blGeo = GeoHash(blHash, precision)
      val trGeo = GeoHash(trHash, precision)
      dxDegrees = trGeo.x - blGeo.x
      dyDegrees = trGeo.y - blGeo.y
      bbox = BoundingBox(Bounds(blGeo.x, trGeo.x), Bounds(blGeo.y,trGeo.y))
    }
  }

  private def createImageBuffer(xdim: Int, ydim: Int) = {
    Array.ofDim[Byte](xdim * ydim)
  }

  val bottomLeft = "bottomLeft"
  val topRight = "topRight"
}
