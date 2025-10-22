/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.parquet.io

import com.google.gson.{GsonBuilder, JsonArray, JsonObject}
import org.geotools.api.feature.`type`.GeometryDescriptor
import org.geotools.api.feature.simple.SimpleFeature
import org.locationtech.geomesa.fs.storage.common.observer.FileSystemObserver
import org.locationtech.geomesa.fs.storage.parquet.io.GeoParquetMetadata.ColumnMetadata
import org.locationtech.geomesa.fs.storage.parquet.io.GeometrySchema.GeometryEncoding
import org.locationtech.geomesa.utils.geotools.ObjectType
import org.locationtech.geomesa.utils.geotools.ObjectType.ObjectType
import org.locationtech.geomesa.utils.text.StringSerialization
import org.locationtech.jts.geom.{Envelope, Geometry, GeometryCollection, Point}

import java.util.{Collections, Locale}

/**
 * Model for GeoParquet file metadata
 *
 * @param primaryGeometry name of the primary geometry in the file
 * @param geometries information about each geometry column in the file
 */
case class GeoParquetMetadata(primaryGeometry: String, geometries: Seq[ColumnMetadata]) {
  def toJson(): String = GeoParquetMetadata.toJson(this)
}

/**
 * Class for generating geoparquet metadata
 */
object GeoParquetMetadata {

  import StringSerialization.{alphaNumericSafeString, decodeAlphaNumericSafeString}

  import scala.collection.JavaConverters._

  val GeoParquetMetadataKey = "geo"

  private val gson = new GsonBuilder().disableHtmlEscaping().create()

  case class ColumnMetadata(
    name: String,
    encoding: GeoParquetColumnEncoding.Value,
    types: Seq[GeoParquetColumnType.Value],
    covering: Option[String],
    bounds: Envelope = new Envelope(),
  )

  object GeoParquetColumnEncoding extends Enumeration {

    type GeoParquetColumnEncoding = Value
    val WKB, point, linestring, polygon, multipoint, multilinestring, multipolygon = Value

    /**
     * Gets the geometry object type associated with the column encoding
     *
     * @param encoding encoding
     * @return
     */
    def toObjectType(encoding: GeoParquetColumnEncoding): ObjectType = {
      encoding match {
        case WKB => ObjectType.GEOMETRY
        case _ => ObjectType.withName(encoding.toString.toUpperCase(Locale.US))
      }
    }
  }

  object GeoParquetColumnType extends Enumeration {

    type GeoParquetColumnType = Value
    val Point, LineString, Polygon, MultiPoint, MultiLineString, MultiPolygon, GeometryCollection = Value
    val `Point Z`, `LineString Z`, `Polygon Z`, `MultiPoint Z`, `MultiLineString Z`, `MultiPolygon Z`, `GeometryCollection Z` = Value

    /**
     * Gets the geometry object type based on the GeoParquet column types
     *
     * @param colTypes column types from GeoParquet metadata
     * @return
     */
    def toObjectType(colTypes: Seq[GeoParquetColumnType]): ObjectType = {
      if (colTypes.lengthCompare(1) != 0) {
        ObjectType.GEOMETRY // more than 1 geometry type
      } else if (colTypes.head == GeometryCollection || colTypes.head == `GeometryCollection Z` ) {
        ObjectType.GEOMETRY_COLLECTION // this is the one objectType whose name doesn't align with the columnType
      } else {
        ObjectType.withName(colTypes.head.toString.replace(" Z", "").toUpperCase(Locale.US))
      }
    }
  }

  /**
   * Generate json describing the GeoParquet file, conforming to the 1.1.0 GeoParquet schema
   *
   * @param metadata metadata
   * @return
   */
  def toJson(metadata: GeoParquetMetadata): String = {
    val model = new JsonObject()
    model.addProperty("version", "1.1.0")
    model.addProperty("primary_column", metadata.primaryGeometry)
    val cols = model.addObject("columns")
    metadata.geometries.foreach { column =>
      val metadata = cols.addObject(column.name)
      metadata.addProperty("encoding", column.encoding.toString)
      val types = metadata.addArray("geometry_types", 1)
      column.types.foreach(t => types.add(t.toString))
      if (!column.bounds.isNull) {
        val bbox = metadata.addArray("bbox", 4)
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
   * Parse a GeoParquet json into a model object
   *
   * @param json json string
   * @return
   */
  def fromJson(json: String): GeoParquetMetadata = {
    val obj = gson.fromJson(json, classOf[JsonObject])
    val primary = obj.get("primary_column").getAsString
    val cols = obj.getAsJsonObject("columns").entrySet().asScala.map { entry =>
      val name = entry.getKey
      val obj = entry.getValue.getAsJsonObject
      // required entries
      val encoding = GeoParquetColumnEncoding.withName(obj.get("encoding").getAsString)
      val types = obj.getAsJsonArray("geometry_types").asList().asScala.map(t => GeoParquetColumnType.withName(t.getAsString))
      // optional entries
      // note: all covering fields *must* refer to the same column, so we only pull out one
      val covering =
        Option(obj.getAsJsonObject("covering")).map(_.getAsJsonObject("bbox").getAsJsonArray("xmin").get(0).getAsString)
      val bounds = new Envelope()
      Option(obj.getAsJsonArray("bbox")).foreach { bbox =>
        bounds.expandToInclude(bbox.get(0).getAsDouble, bbox.get(1).getAsDouble)
        bounds.expandToInclude(bbox.get(2).getAsDouble, bbox.get(3).getAsDouble)
      }
      // TODO crs, orientation, edges, epoch
      ColumnMetadata(name, encoding, types.toSeq, covering, bounds)
    }
    GeoParquetMetadata(primary, cols.toSeq)
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
        Collections.singletonMap(GeoParquetMetadataKey, GeoParquetMetadata(primary, columns.toSeq).toJson())
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
          GeoParquetColumnEncoding.WKB
        } else {
          GeoParquetColumnEncoding.withName(binding.getSimpleName.toLowerCase(Locale.US))
        }
      }
      // TODO add z for 3d points
      val types = if (binding == classOf[Geometry]) { Seq.empty } else { Seq(GeoParquetColumnType.withName(binding.getSimpleName)) }
      val covering = schema.boundingBoxes.collectFirst { case b if b.geometry == name => b.bbox }
      ColumnMetadata(name, encoding, types, covering)
    }

    override def flush(): Unit = {}
    override def close(): Unit = {}
  }
}
