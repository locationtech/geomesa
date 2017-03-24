/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.export.formats

import java.io.OutputStream

import org.geotools.data.simple.SimpleFeatureCollection
import org.locationtech.geomesa.filter.function.BinaryOutputEncoder.{EncodingOptions, LatLonAttributes, encodeFeatureCollection}
import org.locationtech.geomesa.tools.export.BinExportParams

class BinExporter(os: OutputStream,
                  dtgAttribute: String,
                  idAttribute: Option[String],
                  latAttribute: Option[String],
                  lonAttribute: Option[String],
                  lblAttribute: Option[String]) extends FeatureExporter {

  val id = idAttribute.orElse(Some("id"))
  val latLon = for (lat <- latAttribute; lon <- lonAttribute) yield { LatLonAttributes(lat, lon) }

  override def export(fc: SimpleFeatureCollection): Option[Long] = {
    encodeFeatureCollection(fc, os, EncodingOptions(latLon, Option(dtgAttribute), id, lblAttribute))
    None
  }

  override def flush(): Unit = os.flush()

  override def close(): Unit = os.close()
}

object BinExporter {

  private def date(params: BinExportParams, dtg: Option[String]): String =
    Option(params.dateAttribute).orElse(dtg).getOrElse("dtg")

  def getAttributeList(params: BinExportParams, dtg: Option[String]): Seq[String] =
    Seq(params.latAttribute, params.lonAttribute, params.idAttribute, date(params, dtg), params.labelAttribute).filter(_ != null)

  def apply(os: OutputStream, params: BinExportParams, dtg: Option[String]) =
    new BinExporter(os,
      date(params, dtg),
      Option(params.idAttribute),
      Option(params.latAttribute),
      Option(params.lonAttribute),
      Option(params.labelAttribute))
}
