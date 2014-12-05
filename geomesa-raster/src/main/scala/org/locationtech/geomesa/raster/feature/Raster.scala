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

package org.locationtech.geomesa.raster.feature

import java.awt.image.RenderedImage

import org.joda.time.DateTime
import org.locationtech.geomesa.raster.util.RasterUtils
import org.locationtech.geomesa.utils.geohash.{BoundingBox, GeoHash}

class Raster(id: String) extends Serializable {
  private var cachedChunk = None: Option[RenderedImage]
  private var bbox = None: Option[BoundingBox]
  private var mbgh = None: Option[GeoHash]
  private var band = None: Option[String]
  private var resolution = None: Option[Double]
  private var units = None: Option[String]
  private var dataType = None: Option[String]
  private var timeStamp = None: Option[DateTime]

  def setBand(b: String) = band = Some(b)
  def setResolution(d: Double) = resolution = Some(d)
  def setUnits(u: String) = units = Some(u)
  def setDataType(dType: String) = dataType = Some(dType)

  def setMbgh(mbGeoHash: GeoHash) = mbgh = Some(mbGeoHash)
  def getMbgh = mbgh match {
    case Some(m) => m
    case _       => throw UninitializedFieldError("Error, no minimum bounding box geohash")
  }

  def setTime(time: DateTime) = timeStamp = Some(time)
  def getTime = timeStamp match {
    case Some(t) => t
    case _ => throw UninitializedFieldError("Error, no timestamp")
  }
  
  def setBoundingBox(boundingBox: BoundingBox) = bbox = Some(boundingBox)
  def getBounds: BoundingBox = bbox match {
    case Some(box) => box
    case _ => throw UninitializedFieldError("Error, no boundingbox")
  }

  def setChunk(chunk: RenderedImage) = cachedChunk = Some(chunk)
  def getChunk: RenderedImage = cachedChunk match {
    case Some(image) => image
    case _ => throw UninitializedFieldError("Error, no raster data available ")
  }

  def encodeValue = RasterUtils.imageSerialize(getChunk)

  def encodeMetaData = ???

  def getID = id
  def getName = getID
  def getBand = band
  def getResolution = resolution
  def getUnits = units

}