/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.export.formats

import java.util.Locale

import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.opengis.feature.simple.SimpleFeatureType

/**
  * Export format enumeration
  *
  * @param name common name
  * @param extensions file extensions corresponding to the format
  * @param streaming does this format support writing to an output stream, or not (i.e. requires a random-access file)
  * @param countable does this format accurately count bytes as they're written, or not (i.e. only after `close()`)
  * @param bytesPerAttribute estimated bytes per attribute
  */
sealed abstract class ExportFormat(
    val name: String,
    val extensions: Seq[String],
    val streaming: Boolean,
    val countable: Boolean,
    bytesPerAttribute: Float) {

  val bytesPerFeatureProperty = SystemProperty(s"org.locationtech.geomesa.export.$name.bytes-per-feature")

  /**
    * Estimated bytes per feature
    *
    * @param sft simple feature type
    * @return
    */
  def bytesPerFeature(sft: SimpleFeatureType): Float = {
    bytesPerFeatureProperty.toFloat.orElse(ExportFormat.BytesPerFeatureProperty.toFloat).getOrElse {
      (sft.getAttributeCount + 1) * bytesPerAttribute // +1 for feature id
    }
  }

  override def toString: String = name
}

object ExportFormat {

  val BytesPerFeatureProperty = SystemProperty("org.locationtech.geomesa.export.bytes-per-feature")

  val Formats: Seq[ExportFormat] = Seq(Arrow, Avro, Bin, Csv, Gml2, Gml3, Json, Leaflet, Null, Orc, Parquet, Shp, Tsv)

  def apply(name: String): Option[ExportFormat] =
    Formats.find(f => f.name.equalsIgnoreCase(name) || f.extensions.contains(name.toLowerCase(Locale.US)))

  // note: these estimated sizes are based on exporting 86602 gdelt records from 2018-01-01
  // with the attributes GLOBALEVENTID,SQLDATE,MonthYear,Actor1Code,Actor1Name,dtg,geom

  case object Arrow extends ExportFormat("arrow", Seq("arrow"), true, true, 6.6f) with ArrowBytesPerFeature

  case object Avro extends ExportFormat("avro", Seq("avro"), true, true, 1.9f)

  case object Bin extends ExportFormat("bin", Seq("bin"), true, true, -1) with BinBytesPerFeature

  case object Csv extends ExportFormat("csv", Seq("csv"), true, true, 12)

  case object Json extends ExportFormat("json", Seq("json", "geojson"), true, true, 32)

  case object Gml2 extends ExportFormat("gml2", Seq("gml2"), true, false, 78)

  case object Gml3 extends ExportFormat("gml", Seq("xml", "gml", "gml3"), true, false, 73)

  case object Leaflet extends ExportFormat("leaflet", Seq("html"), true, true, 32)

  case object Null extends ExportFormat("null", Seq.empty, true, true, 0)

  case object Orc extends ExportFormat("orc", Seq("orc"), false, false, 1.4f)

  case object Parquet extends ExportFormat("parquet", Seq("parquet"), false, false, 1.6f)

  case object Shp extends ExportFormat("shp", Seq("shp"), false, false, 105)

  case object Tsv extends ExportFormat("tsv", Seq("tsv"), true, true,  12)

  trait ArrowBytesPerFeature extends ExportFormat {
    abstract override def bytesPerFeature(sft: SimpleFeatureType): Float = {
      val base = super.bytesPerFeature(sft)
      if (sft == org.locationtech.geomesa.arrow.ArrowEncodedSft) {
        base * 1000 // default batch size
      } else {
        base
      }
    }
  }

  trait BinBytesPerFeature extends ExportFormat {
    override def bytesPerFeature(sft: SimpleFeatureType): Float = {
      val base = bytesPerFeatureProperty.toInt.orElse(ExportFormat.BytesPerFeatureProperty.toInt).getOrElse {
        // note: assume there is a label field, which, if wrong, should cause us
        // to under-count but then adjust and keep writing
        24
      }
      if (sft == BinaryOutputEncoder.BinEncodedSft) {
        base * 1000 // default batch size
      } else {
        base
      }
    }
  }
}
