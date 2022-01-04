/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.export.formats

import java.io.OutputStreamWriter

import com.google.gson.stream.JsonWriter
import org.locationtech.geomesa.features.serialization.GeoJsonSerializer
import org.locationtech.geomesa.tools.`export`.formats.FeatureExporter.ExportStream
import org.locationtech.geomesa.tools.export.formats.FeatureExporter.ByteCounterExporter
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

class GeoJsonExporter(stream: ExportStream) extends ByteCounterExporter(stream) {

  private var writer: JsonWriter = _
  private var serializer: GeoJsonSerializer = _

  override def start(sft: SimpleFeatureType): Unit = {
    writer = GeoJsonSerializer.writer(new OutputStreamWriter(stream.os))
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
        stream.os.write('\n')
      }
    } finally {
      if (writer != null) {
        writer.close() // also closes underlying writer and output stream
      }
    }
  }
}
