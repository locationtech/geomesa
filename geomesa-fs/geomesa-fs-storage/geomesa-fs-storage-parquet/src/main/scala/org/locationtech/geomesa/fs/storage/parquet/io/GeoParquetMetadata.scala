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
import org.geotools.api.feature.simple.SimpleFeature
import org.locationtech.geomesa.fs.storage.common.observer.FileSystemObserver
import org.locationtech.geomesa.fs.storage.parquet.io.GeometrySchema.GeometryEncoding
import org.locationtech.geomesa.utils.text.StringSerialization
import org.locationtech.jts.geom.{Envelope, Geometry, GeometryCollection, Point}

import java.util.{Collections, Locale}

/**
 * Class for generating geoparquet metadata
 */
object GeoParquetMetadata {

  import StringSerialization.{alphaNumericSafeString, decodeAlphaNumericSafeString}

  val GeoParquetMetadataKey = "geo"

  private val gson = new GsonBuilder().disableHtmlEscaping().create()

  private case class ColumnMetadata(
    name: String,
    encoding: String,
    types: Option[String],
    covering: Option[String],
    bounds: Envelope = new Envelope(),
  )

  /**
   * Generate json describing the GeoParquet file, conforming to the 1.1.0 GeoParquet schema
   *
   * @param primaryGeometry name of the primary geometry in the file
   * @param geometries pairs of geometries and optional bounds
   * @return
   */
  private def apply(primaryGeometry: String, geometries: Seq[ColumnMetadata]): String = {
    val model = new JsonObject()
    model.addProperty("version", "1.1.0")
    model.addProperty("primary_column", primaryGeometry)
    val cols = model.addObject("columns")
    geometries.foreach { column =>
      val metadata = cols.addObject(column.name)
      metadata.addProperty("encoding", column.encoding)
      val types = metadata.addArray("geometry_types", 1) // TODO add ' Z' for 3d points
      column.types.foreach(types.add)
      if (!column.bounds.isNull) {
        val bbox = metadata.addArray("bbox", 4) // TODO add z for 3d points
        bbox.add(column.bounds.getMinX)
        bbox.add(column.bounds.getMinY)
        bbox.add(column.bounds.getMaxX)
        bbox.add(column.bounds.getMaxY)
      }
      column.covering.foreach { c =>
        val bbox = metadata.addObject("covering").addObject("bbox")
        Seq("xmin", "ymin", "xmax", "ymax").foreach { name =>
          val bound = bbox.addArray(name, 2)
          bound.add(c)
          bound.add(name)
        }
      }
    }

    gson.toJson(model)
  }

  /**
   * Helper for building models
   *
   * @param o object
   */
  private implicit class RichJsonObject(val o: JsonObject) extends AnyVal {
    def addArray(key: String, size: Int): JsonArray = {
      val array = new JsonArray(size)
      o.add(key, array)
      array
    }
    def addObject(key: String): JsonObject = {
      val obj = new JsonObject()
      o.add(key, obj)
      obj
    }
  }

  /**
   * Observer class that generates GeoParquet metadata
   *
   * @param schema simple feature schema
   */
  class GeoParquetObserver(schema: SimpleFeatureParquetSchema) extends FileSystemObserver {

    import scala.collection.JavaConverters._

    private val columns =
      schema.sft.getAttributeDescriptors.asScala.collect { case d: GeometryDescriptor if isGeoParquet(d) => createColumnMetadata(d) }
    private val boundsWithIndex = columns.map(c => c.bounds -> schema.sft.indexOf(decodeAlphaNumericSafeString(c.name)))

    /**
     * Gets the file metadata for geoparquet
     *
     * @return
     */
    def metadata(): java.util.Map[String, String] = {
      if (columns.isEmpty) { Collections.emptyMap() } else {
        val primary = {
          // there's a chance that the primary geom wasn't encoded in the file, but other geoms were
          val expected = alphaNumericSafeString(schema.sft.getGeometryDescriptor.getLocalName)
          columns.find(_.name == expected).getOrElse(columns.head).name
        }
        Collections.singletonMap(GeoParquetMetadataKey, GeoParquetMetadata(primary, columns.toSeq))
      }
    }

    override def write(feature: SimpleFeature): Unit = {
      boundsWithIndex.foreach { case (bounds, i) =>
        val geom = feature.getAttribute(i).asInstanceOf[Geometry]
        if (geom != null) {
          bounds.expandToInclude(geom.getEnvelopeInternal)
        }
      }
    }

    /**
     * Checks if a geometry is encoded as GeoParquet
     *
     * @param d descriptor
     * @return
     */
    private def isGeoParquet(d: GeometryDescriptor): Boolean =
      schema.encodings.geometry == GeometryEncoding.GeoParquetNative ||
        schema.encodings.geometry == GeometryEncoding.GeoParquetWkb ||
        d.getType.getBinding == classOf[Point] ||
        d.getType.getBinding == classOf[GeometryCollection] ||
        d.getType.getBinding == classOf[Geometry]

    /**
     * Create metadata placeholder for geometry columns
     *
     * @param descriptor geometry descriptor
     * @return
     */
    private def createColumnMetadata(descriptor: GeometryDescriptor): ColumnMetadata = {
      val name = alphaNumericSafeString(descriptor.getLocalName)
      val binding = descriptor.getType.getBinding
      val encoding = {
        // for non-wkb encoding schemes, we still use WKB for mixed-type geometries
        if (schema.encodings.geometry == GeometryEncoding.GeoParquetWkb ||
            binding == classOf[Geometry] || binding == classOf[GeometryCollection]) {
          "WKB"
        } else {
          binding.getSimpleName.toLowerCase(Locale.US)
        }
      }
      val types = if (binding == classOf[Geometry]) { None } else { Some(binding.getSimpleName) }
      val covering = schema.boundingBoxes.collectFirst { case b if b.geometry == name => b.bbox }
      ColumnMetadata(name, encoding, types, covering)
    }

    override def flush(): Unit = {}
    override def close(): Unit = {}
  }
}
