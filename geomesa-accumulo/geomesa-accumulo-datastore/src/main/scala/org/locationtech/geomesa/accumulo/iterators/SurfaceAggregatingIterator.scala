/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.iterators

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
