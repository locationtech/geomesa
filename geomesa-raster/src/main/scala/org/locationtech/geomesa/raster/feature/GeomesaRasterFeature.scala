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

import org.geotools.coverage.grid.GridCoverage2D
import org.geotools.geometry.Envelope2D
import org.geotools.geometry.jts.ReferencedEnvelope
import org.joda.time.DateTime
import org.locationtech.geomesa.raster.util.RasterUtils.renderedImageToGridCoverage2d
import org.locationtech.geomesa.utils.geohash.GeoHash
import org.opengis.filter.identity.FeatureId
import org.opengis.geometry.{BoundingBox, Envelope}

class GeomesaRasterFeature(id: FeatureId) extends Serializable {
  
  private var cachedChunk = None: Option[RenderedImage]
  private var envelope = None: Option[Envelope2D]
  private var mbGeoHash = None: Option[GeoHash]
  private var cachedGC = None: Option[GridCoverage2D]
  private var band = None: Option[String]
  private var resolution = None: Option[Double]
  private var units = None: Option[String]
  private var dataType = None: Option[String]
  private var timeStamp = None: Option[DateTime]

  def setBand(b: String) = band = Some(b)
  def setResolution(d: Double) = resolution = Some(d)
  def setUnits(u: String) = units = Some(u)
  def setDataType(dType: String) = dataType = Some(dType)

  def setMbGeoHash(mbgh: GeoHash) = mbGeoHash = Some(mbgh)
  def getMbGeoHash = mbGeoHash match {
    case Some(m) => m
    case _       => throw UninitializedFieldError("Error, no minimum bounding box geohash")
  }

  def setTime(time: DateTime) = timeStamp = Some(time)
  def getTime = timeStamp match {
    case Some(t) => t
    case _ => throw UninitializedFieldError("Error, no timestamp")
  }
  
  def setEnvelope(env: Envelope2D) = envelope = Some(env)
  def setEnvelope(env: Envelope) = envelope = Some(new Envelope2D(env))

  def getEnvelope: Envelope = envelope.asInstanceOf[Envelope]
  def getEnvelope2D = envelope

  def getBounds: BoundingBox = getEnvelope2D match {
    case Some(e) =>
      new ReferencedEnvelope(e, e.getCoordinateReferenceSystem)
    case _ => throw UninitializedFieldError("Error, no envelope")
  }

  def setChunk(chunk: RenderedImage) = cachedChunk = Some(chunk)
  def getChunk: RenderedImage = cachedChunk match {
    case Some(image) => image
    case _ => throw UninitializedFieldError("Error, no raster data available ")
  }

  // Gets the GC2d if Some(gc) exists, else call the method that optionally sets it and return Some(gc) or None
  def getGridCoverage2d = cachedGC match {
    case Some(gc) => gc
    case _ => setSomeGridCoverage
  }

  private def setSomeGridCoverage = (cachedChunk, envelope) match {
    case (Some(c), Some(e)) =>
      cachedGC = Some(renderedImageToGridCoverage2d(getID, getChunk, getEnvelope))
      cachedGC
    case _ => None
  }

  def getIdentifier = id
  def getID = id.getID
  def getBand = band
  def getResolution = resolution
  def getUnits = units

}