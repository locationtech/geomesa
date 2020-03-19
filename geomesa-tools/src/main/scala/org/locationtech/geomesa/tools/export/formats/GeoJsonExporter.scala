/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.export.formats

import java.io.OutputStream

import org.geotools.data.geojson.GeoJSONWriter
import org.locationtech.geomesa.tools.export.formats.FeatureExporter.{ByteCounter, ByteCounterExporter}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

class GeoJsonExporter(os: OutputStream, counter: ByteCounter) extends ByteCounterExporter(counter) {

  private var writer: GeoJSONWriter = _

  override def start(sft: SimpleFeatureType): Unit = {
    writer = new GeoJSONWriter(os)
  }

  override def export(features: Iterator[SimpleFeature]): Option[Long] = {
    var count = 0L
    while (features.hasNext) {
      writer.write(features.next)
      count += 1L
    }
    // TODO writer.flush()
    Some(count)
  }

  override def close(): Unit = if (writer != null) { writer.close() }
}
