/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.export.formats

import java.io.{OutputStream, OutputStreamWriter}
import java.nio.charset.StandardCharsets

import org.geotools.geojson.feature.FeatureJSON
import org.locationtech.geomesa.tools.export.formats.FeatureExporter.{ByteCounter, ByteCounterExporter}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

class GeoJsonExporter(os: OutputStream, counter: ByteCounter) extends ByteCounterExporter(counter) {

  private val json = new FeatureJSON()
  private val writer = new OutputStreamWriter(os, StandardCharsets.UTF_8)

  private var first = true

  override def start(sft: SimpleFeatureType): Unit = writer.write("""{"type":"FeatureCollection","features":[""")

  override def export(features: Iterator[SimpleFeature]): Option[Long] = {
    var count = 0L
    if (first && features.hasNext) {
      first = false
      writer.write('\n')
      json.writeFeature(features.next, writer)
      count += 1L
    }
    while (features.hasNext) {
      writer.write(",\n")
      json.writeFeature(features.next, writer)
      count += 1L
    }
    writer.flush()
    Some(count)
  }

  override def close(): Unit  = {
    if (!first) {
      writer.write('\n')
    }
    writer.write("]}\n")
    writer.close()
  }
}
