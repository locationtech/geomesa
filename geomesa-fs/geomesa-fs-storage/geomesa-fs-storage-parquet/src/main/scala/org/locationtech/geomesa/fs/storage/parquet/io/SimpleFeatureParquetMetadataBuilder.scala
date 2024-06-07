/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.parquet.io

import org.geotools.api.feature.`type`.GeometryDescriptor
import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.fs.storage.common.jobs.StorageConfiguration
import org.locationtech.geomesa.fs.storage.parquet.io.SimpleFeatureParquetSchema.{Encoding, GeoParquetSchemaKey, SchemaVersionKey}
import org.locationtech.geomesa.utils.geotools.{ObjectType, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.text.StringSerialization.alphaNumericSafeString
import org.locationtech.jts.geom.Envelope

import scala.collection.JavaConverters._

class SimpleFeatureParquetMetadataBuilder(sft: SimpleFeatureType, schemaVersion: Integer) {
  private var geoParquetMetadata: String = null;

  /**
   * See https://geoparquet.org/releases/v1.0.0/schema.json
   *
   * @param sft simple feature type
   * @return
   */
  def withGeoParquetMetadata(envs: Array[Envelope]): SimpleFeatureParquetMetadataBuilder = {
    val geomField = sft.getGeomField

    if (geomField != null) {
      val primaryColumn = alphaNumericSafeString(geomField)
      val columns = {
        val geometryDescriptors = sft.getAttributeDescriptors.toArray.collect {case gd: GeometryDescriptor => gd}
        geometryDescriptors.indices.map(i => columnMetadata(geometryDescriptors(i), envs(i))).mkString(",")
      }

      geoParquetMetadata = s"""{"version":"1.0.0","primary_column":"$primaryColumn","columns":{$columns}}"""
    }

    this
  }

  private def columnMetadata(geom: GeometryDescriptor, bbox: Envelope): String = {
    // TODO "Z" for 3d, minz/maxz for bbox
    val geomTypes = {
      val types = ObjectType.selectType(geom).last match {
        case ObjectType.POINT               => """"Point""""
        case ObjectType.LINESTRING          => """"LineString""""
        case ObjectType.POLYGON             => """"Polygon""""
        case ObjectType.MULTILINESTRING     => """"MultiLineString""""
        case ObjectType.MULTIPOLYGON        => """"MultiPolygon""""
        case ObjectType.MULTIPOINT          => """"MultiPoint""""
        case ObjectType.GEOMETRY_COLLECTION => """"GeometryCollection""""
        case ObjectType.GEOMETRY            => null
      }
      Seq(types).filter(_ != null)
    }
    // note: don't provide crs, as default is EPSG:4326 with longitude first, which is our default/only crs

    def stringify(geomName: String, encoding: String, geometryTypes: Seq[String], bbox: Envelope): String = {
      val bboxString = s"[${bbox.getMinX}, ${bbox.getMinY}, ${bbox.getMaxX}, ${bbox.getMaxY}]"
      s""""$geomName":{"encoding":"$encoding","geometry_types":[${geometryTypes.mkString(",")}],"bbox":$bboxString}"""
    }

    val geomName = alphaNumericSafeString(geom.getLocalName)
    stringify(geomName, Encoding, geomTypes, bbox)
  }

  def build(): java.util.Map[String, String] = {
    Map(
      StorageConfiguration.SftNameKey -> sft.getTypeName,
      StorageConfiguration.SftSpecKey -> SimpleFeatureTypes.encodeType(sft, includeUserData = true),
      SchemaVersionKey                -> schemaVersion.toString,
      GeoParquetSchemaKey             -> geoParquetMetadata
    ).asJava
  }
}
