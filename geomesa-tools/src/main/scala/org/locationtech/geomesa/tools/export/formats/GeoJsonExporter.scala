/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.export.formats

import java.io.{OutputStream, OutputStreamWriter}

import org.locationtech.geomesa.features.serialization.GeoJsonSerializer
import org.locationtech.geomesa.tools.export.formats.FeatureExporter.{ByteCounter, ByteCounterExporter}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

class GeoJsonExporter(os: OutputStream, counter: ByteCounter) extends ByteCounterExporter(counter) {

  private val writer = GeoJsonSerializer.writer(new OutputStreamWriter(os))

  private var serializer: GeoJsonSerializer = _

  override def start(sft: SimpleFeatureType): Unit = {
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
        os.write('\n')
      }
    } finally {
      writer.close() // also closes underlying writer and output stream
    }
  }
}
