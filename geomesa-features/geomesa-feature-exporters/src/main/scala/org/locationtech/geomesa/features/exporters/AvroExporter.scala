/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.features.exporters

import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.locationtech.geomesa.features.SerializationOption.SerializationOption
import org.locationtech.geomesa.features.avro.io.AvroDataFileWriter
import org.locationtech.geomesa.utils.io.CloseWithLogging

import java.io.OutputStream
import java.util.zip.Deflater

class AvroExporter(out: OutputStream, compression: Option[Int], opts: Set[SerializationOption] = Set.empty)
    extends FeatureExporter {

  private var writer: AvroDataFileWriter = _

  override def start(sft: SimpleFeatureType): Unit =
    writer = new AvroDataFileWriter(out, sft, compression.getOrElse(Deflater.DEFAULT_COMPRESSION), opts)

  override def export(features: Iterator[SimpleFeature]): Option[Long] = {
    var count = 0L
    features.foreach { feature =>
      writer.append(feature)
      count += 1L
    }
    writer.flush()
    Some(count)
  }

  override def close(): Unit = {
    CloseWithLogging(Option(writer))
    out.close()
  }
}
