/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.exporters

import com.google.gson.stream.JsonWriter
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.locationtech.geomesa.features.serialization.GeoJsonSerializer

import java.io.{OutputStream, OutputStreamWriter}

class GeoJsonExporter(out: OutputStream) extends FeatureExporter {

  private var writer: JsonWriter = _
  private var serializer: GeoJsonSerializer = _

  override def start(sft: SimpleFeatureType): Unit = {
    writer = GeoJsonSerializer.writer(new OutputStreamWriter(out))
    serializer = new GeoJsonSerializer(sft)
    serializer.startFeatureCollection(writer)
  }

  override def export(features: Iterator[SimpleFeature]): Option[Long] = {
    var count = 0L
    while (features.hasNext) {
      serializer.write(writer, features.next)
      count += 1L
    }
    writer.flush()
    Some(count)
  }

  override def close(): Unit = {
    try {
      if (serializer != null) {
        serializer.endFeatureCollection(writer)
        writer.flush()
        out.write('\n')
      }
    } finally {
      if (writer != null) {
        writer.close() // also closes underlying writer and output stream
      }
    }
  }
}
