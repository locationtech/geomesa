/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.parquet.io

import com.google.gson.{GsonBuilder, JsonArray, JsonObject}
import org.geotools.api.feature.`type`.GeometryDescriptor
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.locationtech.geomesa.fs.storage.common.observer.FileSystemObserver
import org.locationtech.geomesa.utils.geotools.ObjectType
import org.locationtech.geomesa.utils.text.StringSerialization
import org.locationtech.jts.geom.{Envelope, Geometry, GeometryCollection}

import java.util.{Collections, Locale}

/**
 * Class for generating geoparquet metadata
 */
object GeoParquetMetadata {

  val GeoParquetMetadataKey = "geo"

  private val gson = new GsonBuilder().disableHtmlEscaping().create()

  // TODO "covering" - per row bboxes can be used to accelerate spatial queries at the row group/page level

  /**
   * Indicates if an attribute is supported or not.
   *
   * Currently only mixed geometry (encoded as WKB) and point types are encoded correctly as GeoParquet.
   *
   * TODO GEOMESA-3259 support non-point geometries
   *
   * @param d descriptor
   * @return
   */
  private[parquet] def supported(d: GeometryDescriptor): Boolean = {
    ObjectType.selectType(d).last match {
      case ObjectType.POINT | ObjectType.GEOMETRY => true
      case _ => false
    }
  }

  /**
   * Generate json describing the GeoParquet file, conforming to the 1.1.0 GeoParquet schema
   *
   * @param primaryGeometry name of the primary geometry in the file
   * @param geometries pairs of geometries and optional bounds
   * @return
   */
  def apply(primaryGeometry: String, geometries: Seq[(GeometryDescriptor, Option[Envelope])]): String = {
    val cols = new JsonObject()
    geometries.foreach { case (descriptor, bounds) =>
      val types = new JsonArray(1)
      val encoding = descriptor.getType.getBinding match {
        case b if b == classOf[Geometry] => "WKB"
        case b if b == classOf[GeometryCollection] => types.add(classOf[GeometryCollection].getSimpleName); "WKB"
        case b => types.add(b.getSimpleName); b.getSimpleName.toLowerCase(Locale.US)
      }
      val metadata = new JsonObject()
      metadata.addProperty("encoding", encoding)
      metadata.add("geometry_types", types) // TODO add ' Z' for 3d points
      bounds.filterNot(_.isNull).foreach { e =>
        val bbox = new JsonArray(4)
        bbox.add(e.getMinX)
        bbox.add(e.getMinY)
        bbox.add(e.getMaxX)
        bbox.add(e.getMaxY)
        metadata.add("bbox", bbox) // TODO add z for 3d points
      }
      cols.add(StringSerialization.alphaNumericSafeString(descriptor.getLocalName), metadata)
    }

    val model = new JsonObject()
    model.addProperty("version", "1.1.0")
    model.addProperty("primary_column", primaryGeometry)
    model.add("columns", cols)

    gson.toJson(model)
  }

  /**
   * Observer class that generates GeoParquet metadata
   *
   * @param sft simple feature type
   */
  class GeoParquetObserver(sft: SimpleFeatureType) extends FileSystemObserver {

    import scala.collection.JavaConverters._

    private val bounds =
      sft.getAttributeDescriptors.asScala.zipWithIndex.collect {
        case (d: GeometryDescriptor, i) if supported(d) => (d, i, new Envelope())
      }

    def metadata(): java.util.Map[String, String] = {
      if (bounds.isEmpty) { Collections.emptyMap() } else {
        val geoms = bounds.map { case (d, _, env) => (d, Some(env).filterNot(_.isNull)) }
        val primary = bounds.find(_._1 == sft.getGeometryDescriptor).getOrElse(bounds.head)._1.getLocalName
        Collections.singletonMap(GeoParquetMetadataKey, GeoParquetMetadata(primary, geoms.toSeq))
      }
    }

    override def write(feature: SimpleFeature): Unit = {
      bounds.foreach { case (_, i, envelope) =>
        val geom = feature.getAttribute(i).asInstanceOf[Geometry]
        if (geom != null) {
          envelope.expandToInclude(geom.getEnvelopeInternal)
        }
      }
    }

    override def flush(): Unit = {}
    override def close(): Unit = {}
  }
}
