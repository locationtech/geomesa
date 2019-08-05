/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.raster.index

import java.awt.image.RenderedImage
import java.io.{ByteArrayInputStream, ObjectInputStream}
import java.util.Date

import com.typesafe.scalalogging.LazyLogging
import org.locationtech.jts.geom.Geometry
import org.apache.accumulo.core.data.{Key, Value}
import org.apache.hadoop.io.Text
import org.locationtech.geomesa.accumulo.index.IndexValueEncoder.IndexValueEncoderImpl
import org.locationtech.geomesa.features.{ScalaSimpleFeatureFactory, SimpleFeatureSerializer}
import org.locationtech.geomesa.raster._
import org.locationtech.geomesa.raster.data.Raster
import org.locationtech.geomesa.raster.index.RasterEntryDecoder.DecodedIndexValue
import org.locationtech.geomesa.utils.date.DateUtils.toInstant
import org.opengis.feature.simple.SimpleFeature

object RasterEntry {

  val encoder = new ThreadLocal[SimpleFeatureSerializer] {
    override def initialValue(): SimpleFeatureSerializer = new IndexValueEncoderImpl(rasterSft)
  }

  def encodeIndexCQMetadata(metadata: DecodedIndexValue): Array[Byte] = {
    encodeIndexCQMetadata(metadata.id, metadata.geom, metadata.date)
  }

  def encodeIndexCQMetadata(uniqId: String, geometry: Geometry, dtg: Option[Date]): Array[Byte] = {
    val metadata = ScalaSimpleFeatureFactory.buildFeature(rasterSft, List(geometry, dtg.orNull), uniqId)
    encodeIndexCQMetadata(metadata)
  }

  def encodeIndexCQMetadata(sf: SimpleFeature): Array[Byte] = encoder.get.serialize(sf)

  def decodeIndexCQMetadata(k: Key): DecodedIndexValue = {
    decodeIndexCQMetadata(k.getColumnQualifierData.toArray)
  }

  def decodeIndexCQMetadataToSf(cq: Array[Byte]): SimpleFeature = encoder.get.deserialize(cq)

  def decodeIndexCQMetadata(cq: Array[Byte]): DecodedIndexValue = {
    val sf = decodeIndexCQMetadataToSf(cq)
    val id = sf.getID
    val geom = sf.getDefaultGeometry.asInstanceOf[Geometry]
    val dtg = Option(sf.getAttribute(rasterSftDtgName).asInstanceOf[Date])
    DecodedIndexValue(id, geom, dtg, null)
  }
}

object RasterEntryEncoder extends LazyLogging {

  def encode(raster: Raster, visibility: String = ""): (Key, Value) = {

    logger.trace(s"encoding raster: $raster")
    val vis = new Text(visibility)
    val key = new Key(getRow(raster), getCF(raster), getCQ(raster), vis)
    val encodedRaster = encodeValue(raster)

    (key, encodedRaster)
  }

  private def getRow(ras: Raster) = {
    val resEncoder = lexiEncodeDoubleToString(ras.resolution)
    val geohash = ras.minimumBoundingGeoHash.map(_.hash).getOrElse("")
    new Text(s"$resEncoder~$geohash")
  }

  //TODO: WCS: add band value to Raster and insert it into the CF here
  // GEOMESA-561
  private def getCF(raster: Raster): Text = new Text("")

  private def getCQ(raster: Raster): Text = {
    new Text(RasterEntry.encodeIndexCQMetadata(raster.id, raster.metadata.geom, Option(Date.from(toInstant(raster.time)))))
  }

  private def encodeValue(raster: Raster): Value = new Value(raster.serializedChunk)

}

object RasterEntryDecoder {

  case class DecodedIndexValue(id: String, geom: Geometry, date: Option[Date], attributes: Map[String, Any])

  def rasterImageDeserialize(imageBytes: Array[Byte]): RenderedImage = {
    val in: ObjectInputStream = new ObjectInputStream(new ByteArrayInputStream(imageBytes))
    var read: RenderedImage = null
    try {
      read = in.readObject().asInstanceOf[RenderedImage]
    } finally {
      in.close()
    }
    read
  }

  def decode(entry: (Key, Value)) = {
    val renderedImage: RenderedImage = rasterImageDeserialize(entry._2.get)
    val metadata: DecodedIndexValue = RasterEntry.decodeIndexCQMetadata(entry._1)
    val res = lexiDecodeStringToDouble(new String(entry._1.getRowData.toArray).split("~")(0))
    Raster(renderedImage, metadata, res)
  }
}